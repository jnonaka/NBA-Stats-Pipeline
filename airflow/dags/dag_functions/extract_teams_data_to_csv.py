import os
import sys
import pandas as pd
import json

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def extract_to_df(api_response):

    for i in api_response:
        i['conference'] = i['leagues']['standard']['conference']
        i['division'] = i['leagues']['standard']['division']
        del i['leagues']
        
    out = pd.DataFrame(api_response)
    return out[(out['nbaFranchise'] == True) & (out['allStar'] == False)]

def format_csv(df):
    df.to_csv(
        f"{AIRFLOW_HOME}/teams.csv",
        index=False
    )

def main(xcom_json):
    teams_df = extract_to_df(json.loads(xcom_json))
    format_csv(teams_df)

if __name__ == '__main__':
    try:
        xcom_json = json.loads(sys.argv[1])
    except Exception as e:
        print(f"Error detected with input. Error: {e}")
        sys.exit(1)
    
    main(xcom_json)