{% macro pit(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{{ dbt_datavault.validate_pit_metadata(metadata) -}}
{% set tgt = metadata.target -%}

WITH time_line AS (
{%- for src in metadata.sources %}
{%- set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
{%- if not loop.first %}
    {{ dbt_datavault.set_union('dummy') }}
{%- endif %}
    SELECT
      {{ src.hub_key }} AS {{ tgt.hub_key }}
      ,{{ src.effective_ts }} AS {{ tgt.effective_ts }}
    FROM
      {{ src_table }}
{%- endfor %}
) 
{%- for src in metadata.sources %}
{%- set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
, tl__{{ src.table }} AS (
    SELECT
      {{ src.hub_key}}
      ,{{ src.load_dts}}
      ,{{ src.effective_ts}}
      ,COALESCE(LEAD({{ src.effective_ts}}) OVER(PARTITION BY  {{ src.hub_key}} ORDER BY {{ src.effective_ts}} ASC), CAST('9999-12-31' AS TIMESTAMP)) AS effective_end_ts
    FROM 
      {{ src_table }}
){% endfor %}
SELECT  
  tl.{{ tgt.hub_key }}
  ,tl.{{ tgt.effective_ts }}
  ,COALESCE(LEAD(tl.{{ tgt.effective_ts }}) OVER(PARTITION BY tl.{{ tgt.hub_key }} ORDER BY tl.{{ tgt.effective_ts }} ASC), CAST('9999-12-31' AS TIMESTAMP)) AS effective_end_ts
  {%- for src in metadata.sources %}
  ,tl__{{ src.table }}.{{ src.load_dts }} AS tl__{{ src.table }}_{{ src.load_dts }}
  {%- endfor %}
FROM 
  time_line tl
{%- for src in metadata.sources %}
LEFT JOIN tl__{{ src.table }} ON tl.{{ tgt.hub_key }} = tl__{{ src.table }}.{{ src.hub_key }} AND tl__{{ src.table }}.effective_ts <=  tl.{{ tgt.effective_ts }} AND tl.{{ tgt.effective_ts }} < tl__{{ src.table }}.effective_end_ts
{%- endfor -%} 
{% endmacro -%}

{% macro validate_pit_metadata(metadata) -%}
{% set msg = "PIT metadata lacks " -%}
{% if not metadata.target %}{{ exceptions.raise_compiler_error(msg ~ "target") }}{% endif -%}
{% if not metadata.target.hub_key %}{{ exceptions.raise_compiler_error(msg ~ "target.hub_key") }}{% endif -%}
{% if not metadata.target.effective_ts %}{{ exceptions.raise_compiler_error(msg ~ "target.effective_ts") }}{% endif -%}
{% if not metadata.sources %}{{ exceptions.raise_compiler_error(msg ~ "sources") }}{% endif -%}
{% for src in metadata.sources -%}
{% if not src.table %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].table") }}{% endif -%}
{% if not src.hub_key %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].hub_key") }}{% endif -%}
{% if not src.load_dts %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].load_dts") }}{% endif -%}
{% if not src.effective_ts %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].effective_ts") }}{% endif -%}
{% endfor -%}
{% endmacro -%}
