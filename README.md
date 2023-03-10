# NBA-Stats-Pipeline

## Summary
This data pipeline extracts basic NBA data using the [API-NBA](https://rapidapi.com/api-sports/api/api-nba) endpoint hosted on [Rapid API](https://rapidapi.com/). Collected data includes:
- Team data (e.g., name, city, conference, division, etc.)
- Completed NBA games (excludes scheduled and live games)
- Player statistics for each NBA compleged NBA game

This data pipeline should run at no cost if using the GCP free trial given that data pipeline settings are set to extract NBA data for less than 2 seasons

The final output is a [Looker Studio report](TBD) visuzlizing the data collected in GCP BigQuery. 

## Data Pipeline setup Architecture
Below is the overall data pipeline architecture. The pipeline is orchestrated with [Airflow](https://airflow.apache.org) within a [Docker](https://www.docker.com) container.

 ###IMAGE HERE###

1. Create GCP resources with [Terraform](https://www.terraform.io)
2. Extract data from [API-NBA](https://rapidapi.com/api-sports/api/api-nba) and perform basic transformations
3. Load the data into [Google Cloud Storage](google.com)
4. Load the data from cloud storage to BigQuery DW
5. Perform further transformation with [dbt](https://www.getdbt.com)
6. Visualize extracted data using Looker Studio


## Output ([Looker Studio Dashboard](www.google.com))
 
###IMAGE HERE###



## Data Pipeline Setup
The first step 




> **NOTE**: I developed this pipeline using an M1 Mac. There may be further configurations and/or set up required to run this data pipeline for Windows, Linux, or any other OS enviornment.