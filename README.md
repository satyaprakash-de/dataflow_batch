# dataflow_batch

**production-ready Apache Beam (Python) Dataflow batch pipeline that performs below action:**

This code will:
1. Read asset.csv from GCS
2. Parse CSV and calculate MINIMUM_DOWN_PAYMENT = 10% of ASSET_VALUE
3. Append records to BigQuery
4. Move the processed file to the processed folder after successful load

Assumptions:
1. You are using Python Dataflow (Apache Beam)
2. ASSET_VALUE is numeric
3. CSV has a header row
4. BigQuery dataset & table already exist
5. Pipeline runs in batch mode
