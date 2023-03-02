SELECT 
  b.season,
  a.game_id,
  b.game_label,
  b.date_est AS game_date,
  a.team_id,
  a.team_code,
  a.team_name,
  c.conference,
  c.division,
  CASE 
    WHEN a.team_id = b.home_team_id THEN true
    ELSE false
  END AS is_home_team,
  a.* EXCEPT(game_id, team_id, team_code, team_name)
FROM {{ ref('playerstats_stage') }} a
INNER JOIN {{ ref('games_stage') }} b
ON a.game_id = b.game_id
INNER JOIN {{ source("raw", "teams") }} c
ON a.team_id = c.id