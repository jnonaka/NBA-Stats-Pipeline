 {#
    This macro returns the true shooting percentage (TS%) for each player 
#}

{% macro ts_calc(
    points,
    fga,
    fta
) -%}
    CASE 
        WHEN fga + fta = 0 THEN 0
        ELSE (points / (2 * (fga + 0.44 * fta))) * 100
    END 
{%- endmacro %}