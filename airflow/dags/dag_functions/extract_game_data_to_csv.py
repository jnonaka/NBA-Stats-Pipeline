import os
import sys
import pandas as pd
import json

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def extract_to_df(api_response):
    # Extract relavent data from API response
    game_id = [game['id'] for game in api_response]
    season = [game['season'] for game in api_response]
    datetime_utc = [game['date']['start'] for game in api_response]
    away_team_id = [game['teams']['visitors']['id'] for game in api_response]
    away_team_code = [game['teams']['visitors']['code'] for game in api_response]
    home_team_id = [game['teams']['home']['id'] for game in api_response]
    home_team_code = [game['teams']['home']['code'] for game in api_response]
    away_score = [game['scores']['visitors']['points'] for game in api_response]
    home_score = [game['scores']['home']['points'] for game in api_response]
    status = [game['status']['long'] for game in api_response]

    # Create and return dataframe, the game_ids (used to pull player stats from these games),
    # and the season (used to organize GCS bucket and BQ)

    game_df = pd.DataFrame({
        'game_id': game_id, 
        'season': season, 
        'datetime_utc': datetime_utc,
        'away_team_id': away_team_id,
        'away_team_code': away_team_code,
        'home_team_id': home_team_id,
        'home_team_code': home_team_code,
        'away_score': away_score,
        'home_score': home_score,
        'status': status
    })

    # Drop games that are not 'Finished
    game_df = game_df[game_df['status'] == 'Finished'].drop(['status'], axis = 1)

    return game_df, game_id

def format_csv(df, date= None):
    if date is None or date == 'None':
        path = f"{AIRFLOW_HOME}/games.csv"
    else:
        path = f"{AIRFLOW_HOME}/games_{date}.csv"
    print(path)
    df.to_csv(path, index=False)

def main(xcom_json, date = None):
    game_df, game_id  = extract_to_df(json.loads(xcom_json))
    format_csv(game_df, date)
    return game_id

if __name__ == '__main__':
    
    try:
        xcom_json = json.loads(sys.argv[1])
        exec_date = str(sys.argv[2])
    except Exception as e:
        print(f"Error detected with input. Error: {e}")
        sys.exit(1)
    
    main(xcom_json, exec_date)