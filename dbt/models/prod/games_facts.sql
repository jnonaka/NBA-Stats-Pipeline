
SELECT
  a.*,
  b.logo AS home_team_logo,
  c.logo AS away_team_logo,
FROM {{ ref('games_stage')}} a
INNER JOIN {{ source("raw", "teams") }} b
ON a.home_team_id = b.id
INNER JOIN {{ source("raw", "teams") }} c
ON a.away_team_id = c.id