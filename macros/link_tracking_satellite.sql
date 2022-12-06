{% macro link_tracking_satellite(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}
{% set link = metadata.link_source -%}
{% set sat = metadata.sat_source -%}

SELECT
    {{ tgt.link_key }} 
    ,{{ var('load_dts_column', 'load_dts') }}
    ,{{ tgt.effective_ts }} 
    ,COALESCE(LEAD(effective_ts)
        OVER(PARTITION BY {% if link.relationship == 'n-1' %}{{ link.hub_key1 }}{% else %}{{ link.hub_key2 }}{% endif %}
        ORDER BY effective_ts), TO_DATE('9999-12-31', 'YYYY-MM-DD'))
    AS effective_end_ts
    ,{{ var('rec_src_column', 'rec_src') }}
FROM (
SELECT 
    link.{{ link.link_key }} 
    ,link.{{ link.hub_key1 }} 
    ,link.{{ link.hub_key2 }}
    ,sat.{{ var('load_dts_column', 'load_dts') }}
    ,sat.effective_ts
    ,COALESCE(LAG({% if link.relationship == 'n-1' %}link.{{ link.hub_key2 }}{% else %}link.{{ link.hub_key1 }}{% endif %})
        OVER(PARTITION BY {% if link.relationship == 'n-1' %}link.{{ link.hub_key1 }}{% else %}link.{{ link.hub_key2 }}{% endif %} 
        ORDER BY sat.effective_ts), '-1') 
    AS prev_key
    ,sat.{{ var('rec_src_column', 'rec_src') }}
FROM
    {{ ref(link.table) }} link
LEFT JOIN {{ ref(sat.table) }} sat ON link.{{ link.link_key }} = sat.{{ sat.link_key }}
)
WHERE {% if link.relationship == 'n-1' %}{{ link.hub_key2 }} {% else %}{{ link.hub_key1 }} {% endif %} != prev_key
{% endmacro -%}