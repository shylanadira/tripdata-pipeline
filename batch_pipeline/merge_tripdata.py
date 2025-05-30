import os
import pandas as pd
import json

def merge_tripdata():
    print("Starting merge of CSV and JSON trip data...\n")


    csv_dir = "data/csv"
    csv_files = sorted([f for f in os.listdir(csv_dir) if f.endswith(".csv")])
    csv_paths = [os.path.join(csv_dir, f) for f in csv_files]

    if not csv_files:
        print("No CSV files found")
        return

    df_csv = pd.concat([pd.read_csv(f) for f in csv_paths], ignore_index=True)
    print(f"✅ Merged {len(csv_files)} CSV files — Total CSV rows: {len(df_csv)}")


    json_dir = "data/json"
    json_files = sorted([f for f in os.listdir(json_dir) if f.endswith(".json")])
    json_records = []

    for file in json_files:
        path = os.path.join(json_dir, file)
        try:
            with open(path, "r") as f:
                records = json.load(f) 
                if isinstance(records, list):
                    json_records.extend(records)
                else:
                    print(f"Skipped (not a JSON array): {file}")
        except json.JSONDecodeError:
            print(f"Failed to parse: {file}")
            continue

    if json_records:
        df_json = pd.DataFrame(json_records)
        print(f"Merged {len(json_files)} JSON files — Total JSON rows: {len(df_json)}")
    else:
        print("No valid JSON records found.")
        df_json = pd.DataFrame()


    if not df_json.empty:
        common_cols = list(set(df_csv.columns).intersection(df_json.columns))
        df_combined = pd.concat([df_csv[common_cols], df_json[common_cols]], ignore_index=True)
    else:
        df_combined = df_csv

    print(f"\n📊 Final combined rows: {len(df_combined)}")


    output_path = "data/green_tripdata_full.csv"

    desired_order = [
        "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag",
        "RatecodeID", "PULocationID", "DOLocationID", "passenger_count", "trip_distance",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee",
        "improvement_surcharge", "total_amount", "payment_type", "trip_type", "congestion_surcharge"
    ]
    existing_cols = [col for col in desired_order if col in df_combined.columns]
    df_combined = df_combined[existing_cols]

    df_combined.to_csv(output_path, index=False)
    print(f"✅ Final merged file saved to: {output_path}")

if __name__ == "__main__":
    merge_tripdata()