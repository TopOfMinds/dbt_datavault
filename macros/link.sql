{% macro link(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{% set tgt = metadata.target -%}
{% set sources = metadata.sources if metadata.sources else [metadata.source] -%}
{% set all_fields = [tgt.key] + tgt.hub_keys + ['load_dts', 'rec_src'] -%}

{% call dbt_datavault.deduplicate([tgt.key], all_fields) -%}
{% for src in sources %}
{% if not loop.first %}UNION ALL{% endif -%}
{% set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
{% set src_flat_bks = src.hubs_business_keys | sum(start=[]) -%}
SELECT
  {{ dbt_datavault.make_key(src_flat_bks) }} AS {{ tgt.key }}
  {% for hub_business_keys, tgt_hub_key in zip(src.hubs_business_keys, tgt.hub_keys) -%}
  ,{{ dbt_datavault.make_key(hub_business_keys) }} AS {{ tgt_hub_key }}
  {% endfor -%}
  ,{{ src.load_dts }} AS load_dts
  ,'{{ src.rec_src }}' AS rec_src
FROM
  {{ src_table }}
{% if src.filter or is_incremental() -%}
WHERE
  {% set and_ = joiner("AND ") -%}
  {% if src.filter -%}
  {{ and_() }}{{ src.filter }}
  {% endif -%}
  {% if is_incremental() -%}
  {{ and_() }}'{{ var('start_ts') }}' <= {{ src.load_dts }} AND {{ src.load_dts }} < '{{ var('end_ts') }}'
  {% endif -%}
{% endif -%}
{% endfor -%}
{% endcall %}
{% endmacro %}
