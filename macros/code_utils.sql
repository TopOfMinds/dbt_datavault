{% macro load_dts_code(src) -%}
{{ src.load_dts }} AS {{ var('load_dts_column', 'load_dts') -}}
{% endmacro -%}

{% macro rec_src_code(src) -%}
'{{ src.rec_src }}' AS {{ var('rec_src_column', 'rec_src') -}}
{% endmacro -%}

{% macro filter_and_incremental_code(src) -%}
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
{% endmacro -%}
