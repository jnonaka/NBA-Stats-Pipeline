 {#
    This macro returns the Game Score (GmSc) for each player 
#}

{% macro GmSc_calc(
    points,
    fgm,
    fga,
    fta,
    ftm,
    offReb,
    defReb,
    steals,
    assists,
    blocks,
    pFouls,
    turnovers
) -%}
    points + (0.4 * fgm) - (0.7 * fga) - (0.4 * (fta - ftm)) + (0.7 * offReb) + (0.3 * defReb)
        + steals + (0.7 * assists) + (0.7 * blocks) - (0.4 * pFouls) - turnovers
{%- endmacro %}