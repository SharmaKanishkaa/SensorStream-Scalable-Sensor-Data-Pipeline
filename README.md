# SensorStream: Scalable Sensor Data Pipeline with Spark, S3, and Airflow

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

