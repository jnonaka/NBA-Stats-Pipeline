import sys
import os
from google.cloud import storage

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

if __name__ == "__main__":
    try:
        exec_date = sys.argv[1]
        season_val = sys.argv[2]
        data_type = sys.argv[3]
    except Exception as e:
        print(f"Error detected with input. Error {e}")
        sys.exit(1)

    AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
    BUCKET = os.environ.get("GCP_GCS_BUCKET")
    
    if data_type == 'teams':
        OBJECT = f"raw/{season_val}/{data_type}/{data_type}.parquet"    
    else:
        OBJECT = f"raw/{season_val}/{data_type}/{data_type}_{exec_date}.parquet"
    

    if exec_date == 'None':
        LOCAL =  f"{AIRFLOW_HOME}/{data_type}.parquet"
    else:
        LOCAL =  f"{AIRFLOW_HOME}/{data_type}_{exec_date}.parquet"

    upload_to_gcs(
        bucket = BUCKET,
        object_name = OBJECT,
        local_file = LOCAL
    )