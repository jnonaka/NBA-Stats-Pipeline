{{ config(materialized="view") }}

select
    *,
    ROUND({{ eFG_calc('fgm', 'tpm', 'fga') }}, 2) as efgp,
    ROUND({{ ts_calc('points', 'fga', 'fta') }}, 2)   as tsp,
    ROUND({{ GmSc_calc('points', 'fgm', 'fga','fta', 'ftm', 'offReb', 'defReb', 'steals', 'assists', 'blocks', 'pFouls', 'turnovers') }}, 2) as GmSc
from {{ source("raw", "playerstats_2022") }}