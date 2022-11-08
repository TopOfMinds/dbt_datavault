{% macro make_key(ids) -%}
{% set strs = ids[0:1] -%}
{% for str in ids[1:] -%}
{% do strs.extend(["'|'", str]) -%}
{% endfor -%}
CAST({{ dbt.concat(strs) }} AS string)
{%- endmacro %}