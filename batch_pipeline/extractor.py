from google.cloud import storage
import os
from batch_pipeline.config import BUCKET_NAME, GCS_FOLDER, PROJECT_ID

def extract_to_gcs():
    print("Uploading trip data to GCS...")

    client = storage.Client(project=PROJECT_ID)

    try:
        bucket = client.get_bucket(BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' already exists.")
    except Exception:
        print(f"⛔ Bucket not found.")


    files_to_upload = [
        "green_tripdata_full.csv",
        "payment_type.csv",
        "taxi_zone_lookup.csv"
    ]

    for file_name in files_to_upload:
        local_path = os.path.join("data", file_name)
        gcs_path = f"{GCS_FOLDER}/{file_name}"

        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)

        print(f"Uploaded {file_name} → gs://{BUCKET_NAME}/{gcs_path}")

if __name__ == "__main__":
    extract_to_gcs()