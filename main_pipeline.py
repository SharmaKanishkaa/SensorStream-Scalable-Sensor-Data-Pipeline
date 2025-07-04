# === Import Required Modules === # 
# Core PySpark modules for session management, schema definitions, and transformations 
from pyspark.sql import SparkSession, DataFrame 
from pyspark.sql.functions import col, to_timestamp, date_format, regexp_like, broadcast 
from pyspark.sql.types import TimestampType, StructType, StructField, IntegerType, StringType, 
DoubleType, LongType 
from pyspark import StorageLevel  # To cache intermediate DataFrames 
 
# General-purpose Python libraries 
from dataclasses import dataclass 
from typing import List, Dict, Optional 
import datetime 
import json 
import os 
 
# AWS and Airflow integrations 
import boto3 
from botocore.exceptions import ClientError 
from airflow.models import Variable  # For reading Airflow variables 
 
# === Configuration Classes === # 
@dataclass 
class PipelineConfig: 
    """ 
    Stores configuration settings for the data pipeline. 
    Supports AWS/S3, local paths, JDBC configs, file inputs, AQE, and skew handling. 
    """ 
    aws_access_key: str = None 
    aws_secret_key: str = None 
    bucket_name: str = "default-bucket" 
    local_path: str = "/tmp/data" 
    s3_directory: str = "sensor_data/" 
    db_credentials_path: str = "/etc/db_creds.json" 
    input_files: List[str] = None 
    sensor_patterns: List[str] = None 
    default_start_date: str = "2024-01-01" 
    lookback_days: int = 30 
    jdbc_fetch_size: int = 10000 
    jdbc_num_partitions: int = 8 
    write_mode: str = "overwrite" 
    use_aqe: bool = True 
    enable_skew_handling: bool = True 
 
class ConfigManager: 
    """ 
    Handles loading configuration from either Airflow variables or AWS Secrets Manager. 
    """ 
    @staticmethod 
    def load_from_airflow(var_name: str = "sensor_pipeline_config") -> PipelineConfig: 
        config = Variable.get(var_name, deserialize_json=True) 
        return PipelineConfig(**config) 
 
    @staticmethod 
    def load_from_aws_secrets(secret_name: str) -> PipelineConfig: 
        try: 
            client = boto3.client("secretsmanager") 
            response = client.get_secret_value(SecretId=secret_name) 
            return PipelineConfig(**json.loads(response["SecretString"])) 
        except ClientError as e: 
            raise ValueError(f"AWS Secrets Error: {str(e)}") 
 
    @classmethod 
    def load_config(cls, source: str = "airflow", **kwargs) -> PipelineConfig: 
        """ 
        Main config loader entrypoint. 
        :param source: 'airflow' or 'aws' 
        """ 
        if source == "airflow": 
            return cls.load_from_airflow(kwargs.get("var_name")) 
        elif source == "aws": 
            return cls.load_from_aws_secrets(kwargs.get("secret_name")) 
        else: 
            raise ValueError("Invalid config source") 
 
# === Schema Definitions === # 
class PipelineSchemas: 
    """ 
    Defines PySpark schemas for input and output data. 
    """ 
    TAGS = StructType([ 
        StructField("id", IntegerType(), False), 
        StructField("tagpath", StringType(), False), 
        StructField("description", StringType(), True), 
        StructField("unit", StringType(), True) 
    ]) 
 
    SENSOR_RAW = StructType([ 
        StructField("tagid", IntegerType(), False), 
        StructField("t_stamp", LongType(), False), 
        StructField("value", DoubleType(), False), 
        StructField("dataintegrity", IntegerType(), False) 
    ]) 
 
    OUTPUT = StructType([ 
        StructField("datetime", TimestampType(), False), 
        StructField("sensor_value", DoubleType(), True) 
    ]) 
 
# === Data Loading Logic === # 
class DataLoader: 
    """ 
    Loads existing data from local or S3 paths. 
    """ 
    def __init__(self, spark: SparkSession, config: PipelineConfig): 
        self.spark = spark 
        self.config = config 
 
    def try_load(self, paths: List[str], schema: StructType) -> Optional[DataFrame]: 
        """ 
        Tries loading data from multiple locations (local/S3). 
        """ 
        for path in paths: 
            try: 
                df = self.spark.read.schema(schema).parquet(path) 
                if df.rdd.getNumPartitions() > 1: 
                    df = df.coalesce(1) 
                return df 
            except Exception: 
                continue 
        return None 
 
    def load_existing_data(self) -> Dict[str, DataFrame]: 
        """ 
        Loads input files and caches them for reuse. 
        """ 
        data = {} 
        for filename in self.config.input_files: 
            paths = [ 
                os.path.join(self.config.local_path, filename), 
                f"s3a://{self.config.bucket_name}/{self.config.s3_directory}{filename}" 
            ] 
            if df := self.try_load(paths, PipelineSchemas.OUTPUT): 
                data[filename] = df.persist(StorageLevel.MEMORY_AND_DISK) 
        return data 
 
# === JDBC Database Access === # 
class DatabaseManager: 
    """ 
    Handles JDBC connections and SQL queries to a PostgreSQL-compatible source. 
    """ 
    def __init__(self, spark: SparkSession, config: PipelineConfig): 
        self.spark = spark 
        self.config = config 
        with open(config.db_credentials_path) as f: 
            self.db_config = json.load(f)["YourDB"] 
 
    def get_connection_params(self) -> Dict: 
        """ 
        Builds JDBC options dictionary. 
        """ 
        return { 
            "url": f"jdbc:postgresql://{self.db_config['host']}/{self.db_config['dbname']}", 
            "user": self.db_config["user"], 
            "password": self.db_config["password"], 
            "fetchSize": str(self.config.jdbc_fetch_size), 
            "numPartitions": str(self.config.jdbc_num_partitions), 
            "partitionColumn": "tagid", 
            "lowerBound": "1", 
            "upperBound": "100000" 
        } 
 
    def query_table(self, query: str, schema: StructType = None) -> DataFrame: 
        """ 
        Executes a SQL query and returns the result as a DataFrame. 
        """ 
        reader = self.spark.read.format("jdbc").options(**self.get_connection_params()) 
        if schema: 
            reader = reader.schema(schema) 
        return reader.option("query", query).load() 
 
# === Data Transformation Logic === # 
class DataProcessor: 
    """ 
    Transforms raw sensor readings to structured output format. 
    """ 
    def __init__(self, config: PipelineConfig): 
        self.config = config 
 
    def filter_tables(self, tables: List[str], cutoff_date: datetime.datetime) -> List[str]: 
        """ 
        Filters DB table names based on cutoff year and month in their name. 
        Expected table naming convention: `table_YYYY_MM` 
        """ 
        return [t for t in tables  
                if (parts := t.split('_')) and len(parts) > 2 and parts[-2].isdigit() and parts[-1].isdigit() 
                and (int(parts[-2]) > cutoff_date.year or (int(parts[-2]) == cutoff_date.year and 
int(parts[-1]) >= cutoff_date.month))] 
 
    def process_sensor_data(self, df: DataFrame, tags_df: DataFrame) -> DataFrame: 
        """ 
        Joins raw sensor data with tag metadata, filters by integrity, converts timestamps. 
        """ 
        df = self._validate_schema(df, PipelineSchemas.SENSOR_RAW) 
        tags_df = self._validate_schema(tags_df, PipelineSchemas.TAGS) 
        processed = ( 
            df.join(broadcast(tags_df), df.tagid == tags_df.id, "left") 
            .filter(col("dataintegrity") != 0) 
            .withColumn("datetime", to_timestamp(col("t_stamp") / 1000)) 
        ) 
        return self._validate_schema(processed, PipelineSchemas.OUTPUT) 
 
    def _validate_schema(self, df: DataFrame, expected_schema: StructType) -> DataFrame: 
        """ 
        Casts each column to expected type and ensures required columns exist. 
        """ 
        for field in expected_schema: 
            if field.name in df.columns: 
                df = df.withColumn(field.name, col(field.name).cast(field.dataType)) 
            elif not field.nullable: 
                raise ValueError(f"Missing required field: {field.name}") 
        return df.select([field.name for field in expected_schema]) 
 
# === S3 Output Writer === # 
class S3Writer: 
    """ 
    Writes processed DataFrame to S3 in optimized form. 
    """ 
    def __init__(self, spark: SparkSession, config: PipelineConfig): 
        self.spark = spark 
        self.config = config 
 
    def write_optimized(self, df: DataFrame, path: str) -> None: 
        """ 
        Writes partitioned parquet files with record limits for optimized reads. 
        """ 
        (df.repartition(max(1, df.count() // 100000)) 
         .write 
         .mode(self.config.write_mode) 
         .option("maxRecordsPerFile", 100000) 
         .parquet(f"s3a://{self.config.bucket_name}/{path}")) 
 
# === Orchestration Logic === # 
class SensorDataPipeline: 
    """ 
    Main orchestration class combining all components. 
    """ 
    def __init__(self, config: PipelineConfig): 
        self.config = config 
        self.spark = self._init_spark() 
        self.loader = DataLoader(self.spark, config) 
        self.db_manager = DatabaseManager(self.spark, config) 
        self.processor = DataProcessor(config) 
        self.writer = S3Writer(self.spark, config) 
 
    def _init_spark(self) -> SparkSession: 
        """ 
        Initializes Spark with AQE, skew handling, and AWS credentials (if provided). 
        """ 
        builder = (SparkSession.builder 
                   .appName("SensorDataPipeline") 
                   .config("spark.sql.adaptive.enabled", self.config.use_aqe) 
                   .config("spark.sql.adaptive.skewJoin.enabled", self.config.enable_skew_handling) 
                   .config("spark.dynamicAllocation.enabled", "true")) 
 
        if self.config.aws_access_key: 
            builder = (builder 
                       .config("spark.hadoop.fs.s3a.access.key", self.config.aws_access_key) 
                       .config("spark.hadoop.fs.s3a.secret.key", self.config.aws_secret_key)) 
 
        return builder.getOrCreate() 
 
    def run(self) -> None: 
        """ 
        Main entry point to execute the pipeline. 
        """ 
        try: 
            existing_data = self.loader.load_existing_data() 
            final_df = self._process_new_data(existing_data) 
            self._write_outputs(final_df, existing_data) 
        finally: 
            self._cleanup(existing_data) 
 
    def _process_new_data(self, existing_data: Dict) -> DataFrame: 
        """ 
        Reads tag metadata and filters relevant tables and data rows. 
        """ 
        tags_df = self.db_manager.query_table( 
            "SELECT id, tagpath, description, unit FROM your_tags_table", 
            PipelineSchemas.TAGS 
        ) 
 
        # Retrieve all available public table names 
        tables = [row.table_name for row in  
                  self.db_manager.query_table( 
                      "SELECT table_name FROM information_schema.tables WHERE table_schema 
= 'public'").collect()] 
 
        # Compute cutoff based on max datetime of existing files 
        cutoff_date = self._get_cutoff_date(existing_data) 
        filtered_tables = self.processor.filter_tables(tables, cutoff_date) 
 
        # Get list of tag IDs matching the desired patterns 
        ids_str = self._get_matching_ids(tags_df) 
 
        # Read data from relevant tables 
        query = f""" 
        SELECT * FROM {{0}}  
        WHERE tagid IN ({ids_str})  
        AND t_stamp >= {int(cutoff_date.timestamp() * 1000)} 
        """ 
        dfs = [self.db_manager.query_table(query.format(table), PipelineSchemas.SENSOR_RAW) 
for table in filtered_tables] 
        return self.processor.process_sensor_data(dfs[0].unionByName(*dfs[1:]), tags_df) 
 
    def _write_outputs(self, final_df: DataFrame, existing_data: Dict) -> None: 
        """ 
        Writes final outputs partitioned by `tagpath`. 
        """ 
        for tagpath in final_df.select("tagpath").distinct().collect(): 
            tagpath = tagpath[0] 
            col_name = tagpath.replace("/", "_") 
 
            # Rename sensor value column to unique tag name 
            sensor_df = (final_df 
                         .filter(col("tagpath") == tagpath) 
                         .drop("tagpath", "tagid") 
                         .withColumnRenamed( 
                             [c for c in final_df.columns if c not in ["datetime", "tagpath", "tagid"]][0], 
                             col_name 
                         )) 
 
            # Append existing if applicable 
            if self.config.write_mode == "append": 
                if existing_df := existing_data.get(f"{col_name}.parquet"): 
                    sensor_df = existing_df.union(sensor_df) 
 
            output_path = f"{self.config.s3_directory}{col_name}.parquet" 
            self.writer.write_optimized(sensor_df.dropDuplicates(["datetime"]).orderBy("datetime"), 
output_path) 
 
    def _get_cutoff_date(self, existing_data: Dict) -> datetime.datetime: 
        """ 
        Computes the earliest datetime among all cached files, 
        or uses default if no files exist. 
        """ 
        if not existing_data: 
            return datetime.datetime.strptime(self.config.default_start_date, "%Y-%m-%d") 
 
        max_dates = [df.agg({"datetime": "max"}).collect()[0][0] for df in existing_data.values()] 
        return min(max(max_dates), datetime.datetime.now() - 
datetime.timedelta(days=self.config.lookback_days)) 
 
    def _get_matching_ids(self, tags_df: DataFrame) -> str: 
        """ 
        Filters tag IDs using regex patterns defined in config. 
        """ 
        condition = " OR ".join(f"tagpath ~ '{pattern}'" for pattern in self.config.sensor_patterns) 
        result = self.db_manager.query_table(f"SELECT id FROM your_tags_table WHERE 
{condition}") 
        return ",".join(str(row.id) for row in result.collect()) 
 
    def _cleanup(self, data: Dict) -> None: 
        """ 
        Unpersists any cached data and clears Spark's internal cache. 
        """ 
        for df in data.values(): 
            df.unpersist() 
        self.spark.catalog.clearCache() 
 
# === Pipeline Entrypoint === # 
def main(config_source: str = "airflow", **kwargs): 
    """ 
    Entrypoint for executing the pipeline with configurable source (airflow/aws). 
    """ 
    config = ConfigManager.load_config(config_source, **kwargs) 
    pipeline = SensorDataPipeline(config) 
    pipeline.run() 
 
if __name__ == "__main__": 
    main("airflow", var_name="prod_sensor_config")