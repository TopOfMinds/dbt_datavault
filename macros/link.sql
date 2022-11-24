{% macro link(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{{ dbt_datavault.validate_link_metadata(metadata) -}}
{% set tgt = metadata.target -%}
{% set all_fields = [tgt.link_key] + tgt.hub_keys + ['load_dts', 'rec_src'] -%}

{% call dbt_datavault.deduplicate([tgt.link_key], all_fields, no_deduplication=tgt.no_deduplication) -%}
{%- for src in metadata.sources -%}
{% if not loop.first %}UNION ALL{% endif %}
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
{%- endfor -%}
{% endcall %}
{% endmacro %}

{% macro validate_link_metadata(metadata) -%}
{% set msg = "Link metadata lacks " -%}
{% if not metadata.target %}{{ exceptions.raise_compiler_error(msg ~ "target") }}{% endif -%}
{% if not metadata.target.link_key %}{{ exceptions.raise_compiler_error(msg ~ "target.link_key") }}{% endif -%}
{% if not metadata.target.hub_keys %}{{ exceptions.raise_compiler_error(msg ~ "target.hub_keys") }}{% endif -%}
{% if not dbt_datavault.is_list(metadata.target.hub_keys) %}{{ exceptions.raise_compiler_error("target.hub_keys must be a list") }}{% endif -%}
{% if not metadata.sources %}{{ exceptions.raise_compiler_error(msg ~ "sources") }}{% endif -%}
{% for src in metadata.sources -%}
{% if not src.table %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].table") }}{% endif -%}
{% if not src.hub_natural_keys %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].hub_natural_keys ") }}{% endif -%}
{% if not dbt_datavault.is_list(src.hub_natural_keys) %}{{ exceptions.raise_compiler_error("sources[" ~ loop.index0 ~ "].hub_natural_keys must be a list") }}{% endif -%}
{% if not src.hub_natural_keys | length == metadata.target.hub_keys | length %}{{ exceptions.raise_compiler_error("the length of sources[" ~ loop.index0 ~ "].hub_natural_keys must match target.hub_keys") }}{% endif -%}
{% for nks in src.hub_natural_keys -%}
{% set outer_index0 = loop.index0 -%}
{% if not dbt_datavault.is_list(nks) %}{{ exceptions.raise_compiler_error("sources[" ~ outer_index0 ~ "][" ~ loop.index0 ~ "].hub_natural_keys must be a list") }}{% endif -%}
{% endfor -%}
{% if not src.load_dts %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].load_dts") }}{% endif -%}
{% if not src.rec_src %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].rec_src") }}{% endif -%}
{% endfor -%}
{% endmacro -%}