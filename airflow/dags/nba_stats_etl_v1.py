import os
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


from dag_functions.API_NBA_call import call_api
from dag_functions.extract_game_data_to_csv import main as extract_game_data_to_csv_main
from dag_functions.extract_playerstats_data_to_csv import main as extract_playerstats_data_to_csv_main

BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "airflow",
    "start_date": datetime.today() - timedelta(days=7),
    "depends_on_past": False,
    "retries": 1,
}

# Format DAG exectution date as YYYY-MM-DD
exec_date = "{{ execution_date.strftime(\'%Y-%m-%d\') }}"

# Function to determine whether to skip downstream tasks if the first task (games_api_call) returns no data (i.e., no games were played)
def games_api_call_data_check(xcom_json):
    if len(json.loads(xcom_json)) == 0:
        return False
    else:
        return True

with DAG(
    dag_id="nba_games_and_playerstats_v1",
    schedule_interval="@daily",
    is_paused_upon_creation=True,
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['nba-stats'],
) as dag:
    seasons_api_call = PythonOperator(
        task_id = 'seasons_api_call',
        python_callable = call_api,
        op_kwargs = {
            'data_type': 'seasons',
            'input': None
        },
        do_xcom_push=True
    )

    season_val = "{{ ti.xcom_pull(task_ids='seasons_api_call', key='return_value') }}"
    
    games_api_call = PythonOperator(
        task_id = 'games_api_call',
        python_callable = call_api,
        op_kwargs = {
            'data_type': 'games_daily',
            'input': exec_date
        },
        do_xcom_push=True
    )

    # Check whether games_api_call for execution date contains game data
    # If no data exists all downstream tasks are skipped for the execution date
    games_api_call_check = ShortCircuitOperator(
        task_id = 'games_api_call_check',
        python_callable = games_api_call_data_check,
        op_kwargs = {
            'xcom_json': "{{ ti.xcom_pull(task_ids='games_api_call', key='return_value') | tojson}}"
        }
    )

    extract_game_data_to_csv = PythonOperator(
        task_id = 'extract_game_data_to_csv',
        python_callable = extract_game_data_to_csv_main,
        op_kwargs={
            'xcom_json': "{{ ti.xcom_pull(task_ids='games_api_call', key='return_value') | tojson}}",
            'date': exec_date,
        },
        do_xcom_push=True
    )

    game_list = "{{ ti.xcom_pull(task_ids='extract_game_data_to_csv', key='return_value') }}"

    format_to_parquet_game = BashOperator(
        task_id = 'format_to_parquet_game',
        bash_command = f"python {AIRFLOW_HOME}/dags/dag_functions/format_to_parquet.py games {exec_date}" 
    )

    upload_to_gcs_game = BashOperator(
        task_id = 'upload_to_gcs_game',
        bash_command = f"python {AIRFLOW_HOME}/dags/dag_functions/upload_to_gcs.py games {season_val} {exec_date}" 
    )

    transfer_to_bigquery_game = GCSToBigQueryOperator(
        task_id = 'transfer_to_bigquery_game',
        bucket = BUCKET,
        source_objects = [f"raw/2022/games/games_{exec_date}.parquet"],
        source_format = 'PARQUET',
        destination_project_dataset_table = f"nba_data_all.games",
        write_disposition = "WRITE_APPEND",
        schema_fields = [
            {'name': 'game_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'season', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'datetime_utc', 'type': 'TIMESTAMP'},
            {'name': 'away_team_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'away_team_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'away_score', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'home_team_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'home_team_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'home_score', 'type': 'INTEGER', 'mode': 'REQUIRED'}
        ]
    )

    playerstats_api_call = PythonOperator(
        task_id = 'playerstats_api_call',
        python_callable = call_api,
        op_kwargs = {
            'data_type': 'playerstats_daily',
            'input': game_list
        },
        do_xcom_push=True
    )
    
    extract_playerstats_data_to_csv = PythonOperator(
        task_id = 'extract_playerstats_data_to_csv',
        python_callable = extract_playerstats_data_to_csv_main,
        op_kwargs={
            'xcom_json': "{{ ti.xcom_pull(task_ids='playerstats_api_call', key='return_value') | tojson }}",
            'date': exec_date,
        }
    )

    format_to_parquet_playerstats = BashOperator(
        task_id = 'format_to_parquet_playerstats',
        bash_command = f"python {AIRFLOW_HOME}/dags/dag_functions/format_to_parquet.py playerstats {exec_date}" 
    )

    upload_to_gcs_playerstats = BashOperator(
        task_id = 'upload_to_gcs_playerstats',
        bash_command = f"python {AIRFLOW_HOME}/dags/dag_functions/upload_to_gcs.py playerstats {season_val} {exec_date}" 
    )

    transfer_to_bigquery_playerstats = GCSToBigQueryOperator(
        task_id = 'transfer_to_bigquery_playerstats',
        bucket = BUCKET,
        source_objects = [f"raw/{season_val}/playerstats/playerstats_{exec_date}.parquet"],
        source_format = 'PARQUET',
        destination_project_dataset_table = f"nba_data_all.playerstats",
        write_disposition = "WRITE_APPEND",
        schema_fields = [
            {"name": "game_id", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "team_code", "mode": "REQUIRED", "type": "STRING"},
            {"name": "team_id", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "team_name", "mode": "REQUIRED", "type": "STRING"},
            {"name": "player_id", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "player_firstname","mode": "REQUIRED", "type": "STRING"},
            {"name": "player_lastname", "mode": "REQUIRED", "type": "STRING"},
            {"name": "pos", "mode": "REQUIRED", "type": "STRING"},
            {"name": "assists", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "blocks", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "defReb", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "fga", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "fgm", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "fgp", "mode": "REQUIRED", "type": "FLOAT"},
            {"name": "fta", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "ftm", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "ftp", "mode": "REQUIRED", "type": "FLOAT"},
            {"name": "min", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "offReb", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "pFouls", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "plusMinus", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "points", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "steals", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "totReb", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "tpa", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "tpm", "mode": "REQUIRED", "type": "INTEGER"},
            {"name": "tpp", "mode": "REQUIRED", "type": "FLOAT",},
            {"name": "turnovers", "mode": "REQUIRED", "type": "INTEGER"}
        ]
    )

    remove_all_local_files = BashOperator(
            task_id="remove_all_local_files",
            bash_command=f"rm \
                {AIRFLOW_HOME}/games_{exec_date}.csv {AIRFLOW_HOME}/games_{exec_date}.parquet \
                {AIRFLOW_HOME}/playerstats_{exec_date}.csv {AIRFLOW_HOME}/playerstats_{exec_date}.parquet" 
        )

    seasons_api_call >> games_api_call  >> games_api_call_check >> extract_game_data_to_csv >> format_to_parquet_game  \
        >> upload_to_gcs_game >> transfer_to_bigquery_game >> remove_all_local_files
    
    extract_game_data_to_csv >> playerstats_api_call >> extract_playerstats_data_to_csv >> format_to_parquet_playerstats \
        >> upload_to_gcs_playerstats >> transfer_to_bigquery_playerstats >> remove_all_local_files