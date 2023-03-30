import os
import sys
import requests
import time
import json

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Insert own API key here
headers = {
    "X-RapidAPI-Key": os.environ.get('RAPID_API_KEY'),
    "X-RapidAPI-Host": os.environ.get('RAPID_API_HOST')
}

def api_url(data_type):
    if data_type == 'games_daily':
        return 'https://api-nba-v1.p.rapidapi.com/games', 'date'
    elif data_type == 'games_season':
        return 'https://api-nba-v1.p.rapidapi.com/games', 'season'
    elif data_type == 'playerstats_daily':
        return 'https://api-nba-v1.p.rapidapi.com/players/statistics', 'game'
    elif data_type == 'playerstats_season':
        return 'https://api-nba-v1.p.rapidapi.com/players/statistics', ['team', 'season']
    elif data_type == 'teams':
        return 'https://api-nba-v1.p.rapidapi.com/teams', 'league'
    elif data_type == 'seasons':
        return 'https://api-nba-v1.p.rapidapi.com/seasons', 'seasons'
    else:
        print("data_type input error detected. data_type should be one of the following: \
               \'games_daily\', \'games_season\', \'playerstats_daily\', \'playerstats_season\', \'teams\', or \'seasons\'")
        sys.exit(1)

def response_error_check(api_response):
    if len(api_response['errors']) > 0:
        for e in api_response['errors'].keys():
            print(f"{e} error: {api_response['errors'][e]}")
        sys.exit(1)
    else:
        pass

def call_api(data_type, input):

    url, param_key =  api_url(data_type)

    try:
        if param_key == 'seasons':
            response = requests.request(
                "GET",
                url,
                headers=headers
            ).json()
            
            response_error_check(response)

            return response['response'][-1]
            
        elif param_key in ('date', 'league', 'season'):
            response = requests.request(
                "GET", 
                url=url,
                headers=headers, 
                params={param_key: input}
            ).json()
            
            response_error_check(response)
            
            return response['response']

        elif param_key == ['team', 'season']:
            # team_ids are hard coded to current NBA teams
            # The teams are mostly static from season to season unless new teams are added during a future league expansion
            team_ids = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 38, 40, 41]
            response_list = []
            counter = 0

            for id in team_ids:
                response = requests.request(
                    "GET", 
                    url=url,
                    headers=headers, 
                    params={
                        param_key[0]: id,
                        param_key[1]: input
                    }
                ).json()

                response_error_check(response)

                response_list.append(response['response'])
                
                counter+=1
                print(f'Completed {counter}/{len(team_ids)} team-playerstats API calls...')
                
                # Sleep for 6 seconds to avoid exceeding 10 requests per minute on Basic plan (https://rapidapi.com/api-sports/api/api-nba/pricing/)
                # Remove if using paid plan
                time.sleep(6)
                
            return [i for response in response_list for i in response]
        
        elif param_key == 'game':
            input_list = json.loads(input)
            response_list = []
            counter = 0

            for i in input_list:
                response = requests.request(
                    "GET", 
                    url=url,
                    headers=headers, 
                    params={param_key: i}
                ).json()

                response_error_check(response)

                response_list.append(response['response'])
                
                counter+=1
                print(f'Completed {counter}/{len(input_list)} playerstats API calls...')
                # Sleep for 6 seconds to avoid exceeding 10 requests per minute on Basic plan (https://rapidapi.com/api-sports/api/api-nba/pricing/)
                # Remove if using paid plan
                time.sleep(6)
                
            return [i for response in response_list for i in response]

    
    except Exception as e:
        print(f'API connection error detected: {e}')
        print('Note: There is a 10 requests per minute quota for the Basic plan. If the requests per minute quota is being \
              exceeded please wait one minute before running again')
        sys.exit(1)

if __name__ == "__main__":        
    
    try:
        data_type = str(sys.argv[1])
        input = sys.argv[2]
    except Exception as e:
        print(f"Error detected with input. Error {e}")
        sys.exit(1)    
    
    call_api(data_type, input)