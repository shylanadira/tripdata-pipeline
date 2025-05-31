# Capstone Project 3: Batch & Streaming Taxi Data Pipeline

![Project Architecture Diagram](https://github.com/user-attachments/assets/c0b2c0ba-9480-4fe4-a90e-3cb07ea16821)


---

## 📚 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Ingestion](#data-ingestion)
- [Data Transformation](#data-transformation)
- [Virtual Environment](#virtual-environment)
- [Docker & Aiflow](#docker)
- [Folder Structure](#folder-structure)
- [Logging and Monitoring](#logging-and-monitoring)

---

## 🔍 Overview

This project builds an end-to-end **batch and streaming data pipeline** to ingest, transform, and load NYC taxi trip data into BigQuery. It integrates:

- **Python scripts** for extraction, transformation, and loading (ETL)
- **Pub/Sub + Dataflow** for streaming ingestion
- **DBT** for transformation in BigQuery
- **Airflow** for scheduling inside **Docker**

The result is a scalable and automated pipeline with both historical and real-time data support.

---

## ⚙️ Architecture

- **Batch Flow:**  
  CSV + JSON → GCS → BigQuery (`staging_trip_data`) → DBT → `final_trip_data`

- **Streaming Flow:**  
  Python `publisher.py` → Pub/Sub → Dataflow → BigQuery (`trip_data_stream`) → DBT merge

- **Airflow:** schedules all batch tasks daily in Docker

---

## 📥 Data Ingestion

### Batch (Extractor & Loader):

1. **Raw files:**  
   - `data/csv/green_tripdata_0.csv` to `green_tripdata_35.csv`  
   - `data/json/green_tripdata_36.json` to `green_tripdata_52.json`  
   - Lookup: `payment_type.csv`, `time_zone.csv`

2. **Scripts:**
   - `merge_data.py`: Merges all files into `green_tripdata_full.csv`
   - `extractor.py`: Uploads CSVs to GCS
   - `loader.py`: Loads data into `staging_trip_data` in BigQuery

3. **Tools:**  
   - Python `google-cloud-storage`, `google-cloud-bigquery`

---

### Streaming:

1. **Dummy Generator:** `publisher.py`
   - Sends JSON taxi records to Pub/Sub topic `capstone3_streaming_shyla`

2. **Stream Processor:** `beam_pipeline.py`
   - Reads from Pub/Sub → transforms → loads into `trip_data_stream` (BQ)

3. **Schema:** Matches batch schema (with pickup, dropoff, fare, etc.)

---

## 🧪 Data Transformation

Using **DBT**, transformation happens in 2 layers:

### 1. `stg_full_trip_data.sql`  
Appends batch + streaming data into one staging table (incremental)

### 2. `final_trip_data.sql`

Transforms data by:

- ✅ Calculating `trip_duration`  
- ✅ Converting column names to `snake_case`  
- ✅ Joining `payment_type` table for readable labels  
- ✅ Converting `trip_distance` from miles to kilometers  
- ✅ Materialized as an `incremental` table

---

## 🧪 Virtual Environment

```bash
python -m venv venv
.\venv\Scripts\activate
pip install dbt-bigquery apache-beam google-cloud-storage google-cloud-bigquery
```
## Docker & Airflow
### `docker-compose.yaml`

Running Airflow:
```bash
docker-compose up --build
```
Access Airflow at: http://localhost:8080
Login with: admin / admin

DAG:
### `dags/dbt_dag.py`

## Folder Structure
```pgsql
capstone3/
├── data/
│   ├── csv/
│   ├── json/
│   ├── payment_type.csv
│   ├── time_zone.csv
│   └── green_tripdata_full.csv
├── dags/
│   └── dbt_dag.py
├── batch_pipeline/
│   ├── merge_data.py
│   ├── extractor.py
│   └── loader.py
├── streaming_pipeline/
│   ├── publisher.py
│   └── beam_pipeline.py
├── dbt_capstone/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_trip_data.sql
│       │   ├── stg_full_trip_data.sql
│       │   └── stg_trip_stream.sql
│       └── marts/
│           └── final_trip_data.sql
├── secrets/
│   └── key.json
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```
## 📊 Logging and Monitoring

- All Python scripts (`transformer.py`, `extractor.py`, `loader.py`) use the built-in `logging` module.
- Logs track progress during merging, uploading, and transforming processes.
- Airflow UI provides detailed visualization and logs for each task.
- Google Cloud Console enables monitoring for Pub/Sub and Dataflow (streaming) components.



