import os
import sys
import pandas as pd
import json

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def extract_to_df(api_response):
    playerstat_df = pd.DataFrame(api_response)

    out = pd.concat(
        [
            playerstat_df['game'].apply(pd.Series).rename(columns = lambda x: 'game_' + x),
            playerstat_df['player'].apply(pd.Series).rename(columns = lambda x: 'player_' + x),
            playerstat_df['team'].apply(pd.Series).rename(columns = lambda x: 'team_' + x),
            playerstat_df.drop(['game', 'player', 'team'], axis = 1)
        ],
        axis = 1
    ).drop(['team_nickname', 'team_logo', 'comment'], axis = 1)

    out['min'] = pd.to_numeric(out['min'], errors='coerce').astype('Int64')
    out['plusMinus'] = pd.to_numeric(out['plusMinus'], errors='coerce').astype('Int64')
    return out

def format_csv(df, date):
    df.to_csv(
        f"{AIRFLOW_HOME}/playerstats_{date}.csv",
        index=False
    )

def main(xcom_json, date):
    playerstat_df = extract_to_df(json.loads(xcom_json))
    format_csv(playerstat_df, date)

if __name__ == '__main__':
    
    try:
        xcom_json = json.loads(sys.argv[1])
        exec_date = str(sys.argv[2])
    except Exception as e:
        print(f"Error detected with input. Error: {e}")
        sys.exit(1)
    
    main(xcom_json, exec_date)