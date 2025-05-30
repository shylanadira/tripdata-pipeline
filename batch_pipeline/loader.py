from google.cloud import bigquery
from config import PROJECT_ID, BUCKET_NAME, GCS_FOLDER, BQ_DATASET

def load_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    # Check if dataset exists; if not, create it
    dataset_id = f"{PROJECT_ID}.{BQ_DATASET}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except Exception as e:
        print(f"Dataset {dataset_id} not found. Creating it...")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset_id}.")


    files_to_load = {
        "green_tripdata_full.csv": {
            "table": "raw_trip_data",
            "source_format": bigquery.SourceFormat.CSV,
            "partition_field": "lpep_pickup_datetime",
            "skip_leading_rows": 1
        },
        "payment_type.csv": {
            "table": "raw_payment_type",
            "source_format": bigquery.SourceFormat.CSV,
            "skip_leading_rows": 1
        },
        "taxi_zone_lookup.csv": {
            "table": "raw_taxi_zone",
            "source_format": bigquery.SourceFormat.CSV,
            "skip_leading_rows": 1
        }
    }

    for filename, config in files_to_load.items():
        uri = f"gs://{BUCKET_NAME}/{GCS_FOLDER}/{filename}"
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{config['table']}"

        print(f"⏳ Loading {filename} into {table_id}...")

        job_config = bigquery.LoadJobConfig(
            source_format=config["source_format"],
            skip_leading_rows=config.get("skip_leading_rows", 0),
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        if "partition_field" in config:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=config["partition_field"]
            )

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()

        print(f"✅ Loaded {filename} into {table_id}")

if __name__ == "__main__":
    load_to_bigquery()