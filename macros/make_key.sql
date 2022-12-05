{% macro make_key(ids) -%}
    {% set strs = [] -%}
        {% for str in ids -%}
        {%- do strs.append(
            "CAST(" ~ str ~ " AS " ~ dbt.type_string() ~ ")"
        ) -%}

        {%- if not loop.last %}
            {%- do strs.append("'|'") -%}
        {%- endif -%} 

    {% endfor -%}
    CAST({{ dbt.concat(strs) }} AS {{ dbt.type_string() }})
{%- endmacro %}