import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import csv
from io import StringIO
from google.cloud import storage

# ---------------------------
# CONFIGURATION
# ---------------------------
PROJECT_ID = "satyaprakash_dataflow_gcp_project"
REGION = "us-central1"
TEMP_LOCATION = "gs://satyaprakash_dataflow_batch_sample/temp"
STAGING_LOCATION = "gs://satyaprakash_dataflow_batch_sample/staging"

INPUT_FILE = "gs://satyaprakash_dataflow_batch_sample/input/asset.csv"
PROCESSED_FILE = "gs://satyaprakash_dataflow_batch_sample/processed/asset.csv"

BQ_TABLE = (
    "satyaprakash_dataflow_gcp_project:"
    "dp_dataset.asset"
)

# ---------------------------
# CSV PARSER
# ---------------------------
class ParseCSV(beam.DoFn):
    def process(self, element):
        reader = csv.DictReader(StringIO(element))
        for row in reader:
            asset_value = float(row["ASSET_VALUE"])
            row["MINIMUM_DOWN_PAYMENT"] = round(asset_value * 0.10, 2)
            row["ASSET_VALUE"] = asset_value
            yield row

# ---------------------------
# MOVE FILE AFTER SUCCESS
# ---------------------------
def move_gcs_file(src, dest):
    client = storage.Client()
    src_bucket_name, src_blob_name = src.replace("gs://", "").split("/", 1)
    dest_bucket_name, dest_blob_name = dest.replace("gs://", "").split("/", 1)

    src_bucket = client.bucket(src_bucket_name)
    src_blob = src_bucket.blob(src_blob_name)

    dest_bucket = client.bucket(dest_bucket_name)

    src_bucket.copy_blob(src_blob, dest_bucket, dest_blob_name)
    src_blob.delete()

# ---------------------------
# PIPELINE
# ---------------------------
def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        region=REGION,
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        job_name="asset-csv-to-bq",
        save_main_session=True,
    )

    options.view_as(StandardOptions).runner = "DataflowRunner"

    with beam.Pipeline(options=options) as p:

        rows = (
            p
            | "Read CSV" >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
            | "Add Header Back" >> beam.Map(lambda x: 
                "ASSET_ID,ASSET_TYPE,MAKE,MODEL,YEAR,VIN,REGISTRATION_NO,ASSET_VALUE,STATUS\n" + x
            )
            | "Parse CSV" >> beam.ParDo(ParseCSV())
        )

        rows | "Write to BigQuery" >> WriteToBigQuery(
            table=BQ_TABLE,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

    # Move file only after pipeline completes
    move_gcs_file(INPUT_FILE, PROCESSED_FILE)

if __name__ == "__main__":
    run()
