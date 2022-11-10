{% macro hub(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}
{% set all_fields = [tgt.hub_key] + tgt.natural_keys + ['load_dts', 'rec_src'] -%}

{%- call dbt_datavault.deduplicate([tgt.hub_key], all_fields) -%}
{% for src in metadata.sources %}
{% if not loop.first %}UNION ALL{% endif %}
{% set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
SELECT
  {{ dbt_datavault.make_key(src.natural_keys) }} AS {{ tgt.hub_key }}
  {% for src_natural_key, tgt_natural_key in zip(src.natural_keys, tgt.natural_keys) -%}
  ,{{ src_natural_key }} AS {{ tgt_natural_key }}
  {% endfor -%}
  ,{{ dbt_datavault.load_dts_code(src) }}
  ,{{ dbt_datavault.rec_src_code(src) }}
FROM
  {{ src_table }}
{{ dbt_datavault.filter_and_incremental_code(src) }}
{% endfor -%}
{% endcall -%}
{% endmacro %}
