{% macro driving_key_satellite(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{{ dbt_datavault.validate_driving_key_metadata(metadata) -}}
{% set tgt = metadata.target -%}
{% set link = metadata.link_source -%}
{% set sat = metadata.sat_source -%}

SELECT
    {{ tgt.link_key }} 
    ,{{ var('load_dts_column', 'load_dts') }}
    ,{{ tgt.effective_ts }} 
    ,COALESCE(LEAD(effective_ts)
        OVER(PARTITION BY {{ link.driving_key }}
        ORDER BY effective_ts), CAST('9999-12-31' AS TIMESTAMP))
    AS effective_end_ts
    ,{{ var('rec_src_column', 'rec_src') }}
FROM (
    SELECT 
        link.{{ link.link_key }} 
        ,link.{{ link.driving_key }} 
        ,link.{{ link.other_key }}
        ,sat.{{ var('load_dts_column', 'load_dts') }}
        ,sat.effective_ts
        ,COALESCE(LAG(link.{{ link.other_key }})
            OVER(PARTITION BY link.{{ link.driving_key }}
            ORDER BY sat.effective_ts), '-1') 
        AS prev_key
        ,sat.{{ var('rec_src_column', 'rec_src') }}
    FROM
        {{ ref(link.table) }} link
    LEFT JOIN {{ ref(sat.table) }} sat ON link.{{ link.link_key }} = sat.{{ sat.link_key }}
)
WHERE {{ link.other_key }} != prev_key
{% endmacro -%}

{% macro validate_driving_key_metadata(metadata) -%}
{% set msg = "Driving key satellite metadata lacks " -%}
{% if not metadata.target %}{{ exceptions.raise_compiler_error(msg ~ "target") }}{% endif -%}
{% if not metadata.target.link_key %}{{ exceptions.raise_compiler_error(msg ~ "target.link_key") }}{% endif -%}
{% if not metadata.target.effective_ts %}{{ exceptions.raise_compiler_error(msg ~ "target.effective_ts") }}{% endif -%}
{% if not metadata.link_source.table %}{{ exceptions.raise_compiler_error(msg ~ "table") }}{% endif -%}
{% if not metadata.link_source.link_key %}{{ exceptions.raise_compiler_error(msg ~ "link_source.link_key") }}{% endif -%}
{% if not metadata.link_source.driving_key %}{{ exceptions.raise_compiler_error(msg ~ "link_source.driving_key") }}{% endif -%}
{% if not metadata.link_source.other_key %}{{ exceptions.raise_compiler_error(msg ~ "link_source.other_key") }}{% endif -%}
{% if not metadata.sat_source.table %}{{ exceptions.raise_compiler_error(msg ~ "table") }}{% endif -%}
{% if not metadata.sat_source.link_key %}{{ exceptions.raise_compiler_error(msg ~ "link_source.link_key") }}{% endif -%}
{% if not metadata.sat_source.effective_ts %}{{ exceptions.raise_compiler_error(msg ~ "link_source.effective_ts") }}{% endif -%}
{% endmacro -%}