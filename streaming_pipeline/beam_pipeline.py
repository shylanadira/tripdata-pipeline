import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
import json
from datetime import datetime

class ParseMessageFn(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        record['ingestion_timestamp'] = datetime.utcnow().isoformat()
        yield record

PROJECT_ID = 'purwadika'
TOPIC = 'projects/purwadika/topics/capstone3_stream_shyla'
BQ_TABLE = 'purwadika:jcdeol3_capstone3_shyla.trip_data_stream'

# Define the BigQuery table schema
TABLE_SCHEMA = {
    "fields": [
        {"name": "VendorID", "type": "INTEGER"},
        {"name": "lpep_pickup_datetime", "type": "TIMESTAMP"},
        {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP"},
        {"name": "store_and_fwd_flag", "type": "STRING"},
        {"name": "RatecodeID", "type": "INTEGER"},
        {"name": "PULocationID", "type": "INTEGER"},
        {"name": "DOLocationID", "type": "INTEGER"},
        {"name": "passenger_count", "type": "INTEGER"},
        {"name": "trip_distance", "type": "FLOAT"},
        {"name": "fare_amount", "type": "FLOAT"},
        {"name": "extra", "type": "FLOAT"},
        {"name": "mta_tax", "type": "FLOAT"},
        {"name": "tip_amount", "type": "FLOAT"},
        {"name": "tolls_amount", "type": "FLOAT"},
        {"name": "ehail_fee", "type": "FLOAT"},
        {"name": "improvement_surcharge", "type": "FLOAT"},
        {"name": "total_amount", "type": "FLOAT"},
        {"name": "payment_type", "type": "INTEGER"},
        {"name": "trip_type", "type": "INTEGER"},
        {"name": "congestion_surcharge", "type": "FLOAT"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP"}
    ]
}

def run():
    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        region='us-central1',
        job_name='capstone3-streaming-shyla',
        temp_location='gs://purwadhika-dataflow-bucket/temp/',
        runner='DataflowRunner',
        service_account_email= 'jdeol-03@purwadika.iam.gserviceaccount.com'
    )
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=TOPIC)
            | 'Parse JSON Messages' >> beam.ParDo(ParseMessageFn())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                BQ_TABLE,
                schema=TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
