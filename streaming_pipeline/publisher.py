from google.cloud import pubsub_v1
import json
import time
import random
from datetime import datetime, timedelta

# GCP project and topic
PROJECT_ID = "purwadika"
TOPIC_ID = "capstone3_stream_shyla"

topic_path = pubsub_v1.PublisherClient().topic_path(PROJECT_ID, TOPIC_ID)
publisher = pubsub_v1.PublisherClient()

def generate_trip():
    pickup_time = datetime.utcnow()
    duration_minutes = random.randint(2, 30)
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)

    fare_amount = round(random.uniform(3.0, 25.0), 2)
    extra = round(random.choice([0.0, 0.5, 1.0, 1.5]), 2)
    mta_tax = 0.5
    tip_amount = round(random.uniform(0.0, 8.0), 2)
    tolls_amount = round(random.choice([0.0, 2.5, 5.76, 0.0]), 2)
    congestion_surcharge = round(random.choice([0.0, 2.5]), 2)
    improvement_surcharge = 0.3
    total_amount = round(
        fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge,
        2
    )

    return {
        "VendorID": random.choice([1, 2]),
        "lpep_pickup_datetime": pickup_time.strftime("%Y-%m-%d %H:%M:%S"),
        "lpep_dropoff_datetime": dropoff_time.strftime("%Y-%m-%d %H:%M:%S"),
        "store_and_fwd_flag": random.choice(["N", "Y"]),
        "RatecodeID": random.randint(1, 6),
        "PULocationID": random.randint(1, 265),
        "DOLocationID": random.randint(1, 265),
        "passenger_count": random.randint(1, 6),
        "trip_distance": round(random.uniform(0.1, 10.0), 2),
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "ehail_fee": None,
        "improvement_surcharge": improvement_surcharge,
        "total_amount": total_amount,
        "payment_type": random.choice([1, 2, 3, 4]),
        "trip_type": 1,
        "congestion_surcharge": congestion_surcharge
    }

if __name__ == "__main__":
    print(f"🚀 Streaming to topic: {TOPIC_ID}")
    while True:
        message = generate_trip()
        data = json.dumps(message).encode("utf-8")
        publisher.publish(topic_path, data=data)
        print("✅ Published:", message)
        time.sleep(1)