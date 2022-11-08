{% macro pit(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}

WITH time_line AS (
{%- for src in metadata.sources %}
{%- set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
{%- if not loop.first %}
    UNION DISTINCT
{%- endif %}
    SELECT
      {{ src.key }} AS {{ tgt.key }}
      ,{{ src.effective_ts }} AS {{ tgt.effective_ts }}
    FROM
      {{ src_table }}
{%- endfor %}
) 
{%- for src in metadata.sources %}
{%- set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
, {{ src.table }} AS (
    SELECT
      {{ src.key}}
      ,{{ src.load_dts}}
      ,{{ src.effective_ts}}
      ,COALESCE(LEAD({{ src.effective_ts}}) OVER(PARTITION BY  {{ src.key}} ORDER BY {{ src.effective_ts}} ASC), CAST('9999-12-31' AS TIMESTAMP)) AS effective_end_ts
    FROM 
      {{ src_table }}
){% endfor %}
SELECT  
  tl.{{ tgt.key }}
  ,tl.{{ tgt.effective_ts }}
  ,COALESCE(LEAD(tl.{{ tgt.effective_ts }}) OVER(PARTITION BY tl.{{ tgt.key }} ORDER BY tl.{{ tgt.effective_ts }} ASC), CAST('9999-12-31' AS TIMESTAMP)) AS effective_end_ts
  {%- for src in metadata.sources %}
  ,{{ src.table }}.{{ src.load_dts }} AS {{ src.table }}_{{ src.load_dts }}
  {%- endfor %}
FROM 
  time_line tl
{%- for src in metadata.sources %}
LEFT JOIN {{ src.table }} ON tl.{{ tgt.key }} = {{ src.table }}.{{ src.key }} AND {{ src.table }}.effective_ts <=  tl.{{ tgt.effective_ts }} AND tl.{{ tgt.effective_ts }} < {{ src.table }}.effective_end_ts
{%- endfor -%} 
{% endmacro %}