{{ config(materialized="view") }}

select
    *,
    CONCAT(away_team_code, ' vs. ', home_team_code) as game_label,
    DATETIME(datetime_utc, "America/New_York") as datetime_est,
    DATE(DATETIME(datetime_utc, "America/New_York")) as date_est,
    home_score - away_score as home_away_score_differential 
from {{ source("raw", "games_2022") }}