{% macro deduplicate(dedup_fields, return_fields=None, order_field='load_dts') -%}
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
    *
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
      {%- for dedup_field in dedup_fields %}
      COALESCE(CAST(t.{{ dedup_field }} AS STRING), '#') = COALESCE(CAST(q.{{ dedup_field }} AS STRING), '#'){% if not loop.last %} AND{% endif %}
      {%- endfor %} 
  )
  {%- endif %}  
)
WHERE rn = 1
{% endmacro %}