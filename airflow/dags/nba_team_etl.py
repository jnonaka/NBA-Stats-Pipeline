import os
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


from dag_functions.API_NBA_call import call_api
from dag_functions.extract_teams_data_to_csv import main as extract_teams_data_to_csv_main

BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Format DAG exectution date as YYYY-MM-DD
exec_date = "{{ execution_date.strftime(\'%Y-%m-%d\') }}"

with DAG(
    dag_id="nba_teams_data",
    schedule_interval="@once",
    default_args=default_args,
    start_date= datetime.today(),
    catchup=False,
    max_active_runs=1,
    tags=['nba-stats'],
) as dag:
    
    season_val = 2022

    teams_api_call = PythonOperator(
        task_id = 'teams_api_call',
        python_callable = call_api,
        op_kwargs = {
            'input': 'standard',
            'data_type': 'teams'
        },
        do_xcom_push=True
    )
    
    extract_teams_data_to_csv = PythonOperator(
        task_id = 'extract_teams_data_to_csv',
        python_callable = extract_teams_data_to_csv_main,
        op_kwargs={
            'xcom_json': "{{ ti.xcom_pull(task_ids='teams_api_call', key='return_value') | tojson }}",
            'date': exec_date,
        }
    )

    format_to_parquet_teams = BashOperator(
        task_id = 'format_to_parquet_teams',
        bash_command = f"python {AIRFLOW_HOME}/dags/dag_functions/format_to_parquet.py {exec_date} teams" 
    )

    upload_to_gcs_teams = BashOperator(
        task_id = 'upload_to_gcs_teams',
        bash_command = f"python {AIRFLOW_HOME}/dags/dag_functions/upload_to_gcs.py {exec_date} {season_val} teams" 
    )

    transfer_to_bigquery_teams = GCSToBigQueryOperator(
        task_id = 'transfer_to_bigquery_teams',
        bucket = BUCKET,
        source_objects = [f"raw/{season_val}/teams/teams.parquet"],
        source_format = 'PARQUET',
        destination_project_dataset_table = f"nba_data_all.teams",
        write_disposition = "WRITE_TRUNCATE",
        schema_fields = [
            {"name": "id", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "code", "mode": "REQUIRED", "type": "STRING"},
            {"name": "name", "mode": "REQUIRED", "type": "STRING"},
            {"name": "nickname", "mode": "REQUIRED", "type": "STRING"},
            {"name": "city", "mode": "REQUIRED", "type": "STRING"},
            {"name": "logo", "mode": "REQUIRED", "type": "STRING"},
            {"name": "conference","mode": "REQUIRED", "type": "STRING"},
            {"name": "division", "mode": "REQUIRED", "type": "STRING"},
            {"name": "nbaFranchise", "mode": "REQUIRED", "type": "BOOLEAN"},
        ]
    )

    remove_all_local_files = BashOperator(
            task_id="remove_all_local_files",
            bash_command=f"rm {AIRFLOW_HOME}/teams_{exec_date}.csv {AIRFLOW_HOME}/teams_{exec_date}.parquet"
        )
    
    teams_api_call >> extract_teams_data_to_csv >> format_to_parquet_teams >> upload_to_gcs_teams >> transfer_to_bigquery_teams >> remove_all_local_files