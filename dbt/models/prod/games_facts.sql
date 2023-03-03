{{ config(materialized="table") }}

SELECT
  a.*,
  b.logo_url AS home_team_logo,
  c.logo_url AS away_team_logo,
FROM {{ ref('games_stage')}} a
INNER JOIN {{ ref('team_logos') }} b
ON a.home_team_id = b.id
INNER JOIN {{ ref('team_logos') }} c
ON a.away_team_id = c.id