{% macro deduplicate(dedup_fields, return_fields=None, order_field='load_dts', no_deduplication=false, deduplication_include=false) -%}
{% if no_deduplication -%}
{{- caller() }}
{% else -%}
SELECT
{% if return_fields -%}
{% for return_field in return_fields -%}
  {% if not loop.first %}  ,{% else %}  {% endif %}{{ return_field }}
{% endfor -%}
{% else -%}
  * EXCEPT(rn)
{% endif -%}
FROM (
  SELECT
    q.*
    ,ROW_NUMBER() OVER(PARTITION BY {{ dedup_fields | join(', ') }} ORDER BY {{ order_field }} ASC) rn
  FROM (
    {{- caller() | indent(4) }}
  ) q
  {%- if is_incremental() %}
  WHERE NOT EXISTS (
    SELECT 
      1
    FROM
      {{ this }} t
    WHERE
      {%- if deduplication_include %}
      {%- for include in deduplication_include %}
      t.{{ include }} = q.{{ include }}{% if not loop.last %} AND{% endif %}
      {%- endfor -%} 
      {%- else -%}
      {%- for dedup_field in dedup_fields %}
      COALESCE(CAST(t.{{ dedup_field }} AS {{ dbt.type_string() }}), '#') = COALESCE(CAST(q.{{ dedup_field }} AS {{ dbt.type_string() }}), '#'){% if not loop.last %} AND{% endif %}
      {%- endfor -%}
      {%- endif %}   
  )
  {%- endif %}  
)
WHERE rn = 1
{% endif -%}
{% endmacro %}
