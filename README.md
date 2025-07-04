# SensorStream: Sensor Data Pipeline with Spark, S3, and Airflow

A modular and parameterized batch data pipeline for processing industrial sensor data using Apache Spark, orchestrated via Apache Airflow, and integrated with AWS services like S3 and Secrets Manager.

---

## Features

- **Batch Ingestion** of time-series sensor data from PostgreSQL
- **Schema validation** and sensor metadata enrichment
- **Modular design** with components like DataLoader, DataProcessor, DatabaseManager, and S3Writer
- **Spark Optimizations**: AQE, broadcast joins, skew join handling, caching, repartitioning
- **S3 integration** for scalable file storage
- **Airflow DAG** to schedule and orchestrate the ETL workflow
- **Secure credentials** via AWS Secrets Manager and IAM roles
- **Output validation** and idempotent writes

---

## Architecture Overview
![Mermaid Chart](https://github.com/SharmaKanishkaa/SensorStream-Scalable-Sensor-Data-Pipeline/blob/main/Architecture.png)


## ER Diagram
![Mermaid Chart](https://github.com/SharmaKanishkaa/SensorStream-Scalable-Sensor-Data-Pipeline/blob/main/er%20diagram.png)

---

## Objective 

To create a scalable, modular, and optimized pipeline for processing raw sensor data and storing it efficiently for downstream analytics.

Purpose of the Project
Automate Data Collection
Pull sensor data regularly (daily) from a central database, filtering only relevant time windows and sensors.

Clean & Transform Data
Join with metadata (tags_df), filter invalid readings, and convert timestamps.

Efficient Storage
Save cleaned sensor data to Amazon S3 as compressed, partitioned Parquet files — reducing cost and improving query performance for analytics tools like Athena or Redshift Spectrum.

Enable Dynamic Configuration
Use Airflow Variables and AWS Secrets Manager to load runtime parameters (sensor patterns, credentials, etc.) without modifying code.

Support Scalable, Repeatable Workflows
Using Apache Airflow, the pipeline is fully automated and resilient to failures, supporting retries, notifications, and parameterization.

---

## Workflow
1. Configuration Loading
Configuration is loaded from:

Airflow Variables (for file paths, sensor patterns)

AWS Secrets Manager (for secure credentials)

2. Sensor Metadata Fetching
tags_df is loaded — contains mapping of sensor tagid to tagpath and descriptions.

3. Table Filtering
Only tables whose names match the cutoff date (table_YYYY_MM) are queried.

4. Data Extraction
For each filtered table, sensor data is pulled using parallelized JDBC queries based on tagid and t_stamp.

5. Data Transformation
Invalid entries (e.g., dataintegrity = 0) are removed.

Sensor timestamps are converted to readable datetime.

Joined with metadata using broadcast join for efficiency.

6. Writing to S3
Final DataFrame is partitioned by tagpath (e.g., temperature, pressure, energy) and written to S3 in Parquet format.

Files are optimized to cap records per file and prevent duplication.

7. Validation & Orchestration
Airflow DAG validates that S3 files are written.

Separate Airflow DAG refreshes config and credentials weekly.

```

## 🧱 Project Structure

```
sensor-pipeline/
│
├── dags/
│   ├── sensor_pipeline_dag.py        # Main Airflow DAG for pipeline
│   └── config_manager_dag.py         # Airflow DAG for rotating config and secrets
│
├── config/
│   └── pipeline_config.json          # Local JSON fallback config
│
├── src/
│   ├── config_manager.py             # Loads config from Airflow or AWS
│   ├── pipeline.py                   # Orchestration logic (SensorDataPipeline)
│   ├── loader.py                     # DataLoader for local and S3 reads
│   ├── processor.py                  # DataProcessor with transformation logic
│   ├── writer.py                     # S3Writer with optimized output
│   └── db.py                         # JDBC connection manager
│
├── logs/
│   └── run.log                       # Execution logs
│
├── requirements.txt
└── README.md
```

## ⚙️ Installation & Setup

### 1. ✅ Prerequisites

* Python 3.8+
* Java 11
* Apache Spark 3.4.1
* AWS credentials or IAM Role with:

  * S3 Access
  * Secrets Manager
* PostgreSQL instance with sensor data

### 2. 🖥️ Local Setup

```bash
git clone https://github.com/SharmaKanishkaa/SensorStream-Scalable-Sensor-Data-Pipeline-.git
cd SensorStream-Scalable-Sensor-Data-Pipeline

# Install Python dependencies
pip install -r requirements.txt
```

### 3. 🚀 Run Locally (Dev Mode)

```bash
spark-submit \
  --master local[*] \
  src/pipeline.py --config-source file --config-path config/pipeline_config.json
```

---

## ☁️ Airflow Orchestration

### DAG 1: `sensor_data_pipeline`

* Runs daily at 2 AM
* Fetches config
* Executes PySpark pipeline
* Validates S3 output

### DAG 2: `pipeline_config_manager`

* Runs weekly
* Rotates AWS Secrets
* Pulls fresh regex patterns from config service

> 📂 Set Airflow Variables for dynamic config (e.g. `SENSOR_BUCKET`, `SENSOR_PATTERNS`)

---

## 🛠️ Optimization Techniques Used

| Technique                         | Purpose                                |
| --------------------------------- | -------------------------------------- |
| AQE (`spark.sql.adaptive`)        | Dynamic shuffle partitioning and joins |
| Skew Join Handling                | Resolves partition size imbalance      |
| Broadcast Join                    | Efficient small-table joins            |
| Repartition & `maxRecordsPerFile` | Optimized output file sizes            |
| `persist(StorageLevel)`           | Cached intermediate reads from S3      |
| JDBC Partitioning                 | Parallel reads from PostgreSQL         |
| Schema Enforcement                | Validation, pruning, and type safety   |

## 🔐 Security

* Uses **AWS IAM Role** or **AWS Secrets Manager** for credentials
* Configurable via Airflow, AWS, or local JSON
* Secure S3 paths and encryption practices supported
