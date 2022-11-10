{% macro satellite(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{%- set tgt = metadata.target -%}
{% set all_fields = [tgt.hub_key, 'load_dts'] + tgt.attributes + ['rec_src'] -%}

{%- call dbt_datavault.deduplicate([tgt.hub_key] + tgt.attributes, all_fields) %}
{% for src in metadata.sources %}
{% if not loop.first %}UNION ALL{% endif %}
{% set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
SELECT
  {{ dbt_datavault.make_key(src.natural_keys) }} AS {{ tgt.hub_key }}
  ,{{ dbt_datavault.load_dts_code(src) }}
  {% for src_attr, tgt_attr in zip(src.attributes, tgt.attributes) -%}
  ,{{ src_attr }} AS {{ tgt_attr }}
  {% endfor -%}
  ,{{ dbt_datavault.rec_src_code(src) }}
FROM
  {{ src_table }}
{{ dbt_datavault.filter_and_incremental_code(src) }}
{% endfor %}
{% endcall -%}
{% endmacro %}
