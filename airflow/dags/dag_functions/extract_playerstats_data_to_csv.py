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

    out = out[out['min'].notnull()]

    out['game_id'] = pd.to_numeric(out['game_id'], errors='coerce').astype('Int64')
    out['team_id'] = pd.to_numeric(out['team_id'], errors='coerce').astype('Int64')
    out['player_id'] = pd.to_numeric(out['player_id'], errors='coerce').astype('Int64')
    out['assists'] = pd.to_numeric(out['assists'], errors='coerce').astype('Int64')
    out['blocks'] = pd.to_numeric(out['blocks'], errors='coerce').astype('Int64')
    out['defReb'] = pd.to_numeric(out['defReb'], errors='coerce').astype('Int64')
    out['fga'] = pd.to_numeric(out['fga'], errors='coerce').astype('Int64')
    out['fgm'] = pd.to_numeric(out['fgm'], errors='coerce').astype('Int64')
    out['fta'] = pd.to_numeric(out['fta'], errors='coerce').astype('Int64')
    out['ftm'] = pd.to_numeric(out['ftm'], errors='coerce').astype('Int64')
    out['min'] = pd.to_numeric(out['min'], errors='coerce').astype('Int64')
    out['offReb'] = pd.to_numeric(out['offReb'], errors='coerce').astype('Int64')
    out['pFouls'] = pd.to_numeric(out['pFouls'], errors='coerce').astype('Int64')
    out['plusMinus'] = pd.to_numeric(out['plusMinus'], errors='coerce').astype('Int64')
    out['points'] = pd.to_numeric(out['points'], errors='coerce').astype('Int64')
    out['steals'] = pd.to_numeric(out['steals'], errors='coerce').astype('Int64')
    out['totReb'] = pd.to_numeric(out['totReb'], errors='coerce').astype('Int64')
    out['tpa'] = pd.to_numeric(out['tpa'], errors='coerce').astype('Int64')
    out['tpm'] = pd.to_numeric(out['tpm'], errors='coerce').astype('Int64')
    out['turnovers'] = pd.to_numeric(out['turnovers'], errors='coerce').astype('Int64')
    
    return out

def format_csv(df, date = None):
    
    if date is None or date == 'None':
        path = f"{AIRFLOW_HOME}/playerstats.csv"
    else:
        path = f"{AIRFLOW_HOME}/playerstats_{date}.csv"

    df.to_csv(
        path,
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