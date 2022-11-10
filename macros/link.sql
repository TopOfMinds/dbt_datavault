{% macro link(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}
{% set all_fields = [tgt.link_key] + tgt.hub_keys + ['load_dts', 'rec_src'] -%}

{% call dbt_datavault.deduplicate([tgt.link_key], all_fields, no_deduplication=tgt.no_deduplication) -%}
{% for src in metadata.sources %}
{% if not loop.first %}UNION ALL{% endif -%}
{% set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
{% set src_flat_nks = src.hub_natural_keys | sum(start=[]) -%}
SELECT
  {{ dbt_datavault.make_key(src_flat_nks) }} AS {{ tgt.link_key }}
  {% for hub_natural_key, tgt_hub_key in zip(src.hub_natural_keys, tgt.hub_keys) -%}
  ,{{ dbt_datavault.make_key(hub_natural_key) }} AS {{ tgt_hub_key }}
  {% endfor -%}
  ,{{ dbt_datavault.load_dts_code(src) }}
  ,{{ dbt_datavault.rec_src_code(src) }}
FROM
  {{ src_table }}
{{ dbt_datavault.filter_and_incremental_code(src) }}
{% endfor -%}
{% endcall %}
{% endmacro %}
