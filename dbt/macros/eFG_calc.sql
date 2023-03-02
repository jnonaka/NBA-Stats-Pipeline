 {#
    This macro returns the effective field goal percentage (eFG%) for each player 
#}

{% macro eFG_calc(
    fgm,
    tpm,
    fga
) -%}
    CASE 
        WHEN fga = 0 THEN 0
        ELSE ((fgm + 0.5 * tpm) / fga) * 100
    END 
{%- endmacro %}