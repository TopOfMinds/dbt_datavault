{% macro make_key(ids) -%}
    {{ return(adapter.dispatch('make_key','dbt_datavault')(ids)) }}
{%- endmacro %}


{% macro default__make_key(ids) -%}
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


{% macro oracle__make_key(ids) -%}
    {% set strs = [] -%}
    {% for str in ids -%}
        {%- do strs.append(
            "CAST(" ~ str ~ " AS VARCHAR2(256 CHAR))"
        ) -%}

        {%- if not loop.last %}
            {%- do strs.append("'|'") -%}
        {%- endif -%} 

    {% endfor -%}
    CAST({{ dbt.concat(strs) }} AS VARCHAR2(256 CHAR))
{%- endmacro %}