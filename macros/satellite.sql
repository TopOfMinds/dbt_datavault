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
  ,{{ src.load_dts }} AS load_dts
  {% for src_attr, tgt_attr in zip(src.attributes, tgt.attributes) -%}
  ,{{ src_attr }} AS {{ tgt_attr }}
  {% endfor -%}
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
{% endfor %}
{% endcall -%}
{% endmacro %}
