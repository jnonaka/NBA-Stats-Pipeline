import os
import sys
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def format_to_parquet(src_file):
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

if __name__ == "__main__":    
    try:
        data_type = sys.argv[1]
        exec_date = sys.argv[2]
    except Exception as e:
        print(f"Error detected with bash input. Error: {e}")
        sys.exit(1)
    
    if exec_date == 'None' or exec_date is None:
        local_csv_path = f"{AIRFLOW_HOME}/{data_type}.csv"
    else:
        local_csv_path = f"{AIRFLOW_HOME}/{data_type}_{exec_date}.csv"
    
    format_to_parquet(local_csv_path)