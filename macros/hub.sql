{% macro hub(metadata_yaml) -%}
{% set metadata = fromyaml(metadata_yaml) -%}
{{ dbt_datavault.validate_hub_metadata(metadata) -}}
{% set tgt = metadata.target -%}
{% set all_fields = [tgt.hub_key] + tgt.natural_keys + ['load_dts', 'rec_src'] -%}

{%- call dbt_datavault.deduplicate([tgt.hub_key], all_fields, no_deduplication=tgt.no_deduplication) -%}
{% for src in metadata.sources %}
{% if not loop.first %}UNION ALL{% endif %}
{% set src_table = source(src.name, src.table) if src.name else ref(src.table) -%}
SELECT
  {{ dbt_datavault.make_key(src.natural_keys) }} AS {{ tgt.hub_key }}
  {% for src_natural_key, tgt_natural_key in zip(src.natural_keys, tgt.natural_keys) -%}
  ,{{ src_natural_key }} AS {{ tgt_natural_key }}
  {% endfor -%}
  ,{{ dbt_datavault.load_dts_code(src) }}
  ,{{ dbt_datavault.rec_src_code(src) }}
FROM
  {{ src_table }}
{{ dbt_datavault.filter_and_incremental_code(src) }}
{% endfor -%}
{% endcall -%}
{% endmacro -%}

{% macro validate_hub_metadata(metadata) -%}
{% set msg = "Hub metadata lacks " -%}
{% if not metadata.target %}{{ exceptions.raise_compiler_error(msg ~ "target") }}{% endif -%}
{% if not metadata.target.hub_key %}{{ exceptions.raise_compiler_error(msg ~ "target.hub_key") }}{% endif -%}
{% if not metadata.target.natural_keys %}{{ exceptions.raise_compiler_error(msg ~ "target.natural_keys") }}{% endif -%}
{% if metadata.target.natural_keys is string %}{{ exceptions.raise_compiler_error("target.natural_keys must be a list") }}{% endif -%}
{% if not metadata.sources %}{{ exceptions.raise_compiler_error(msg ~ "sources") }}{% endif -%}
{% for src in metadata.sources -%}
{% if not src.table %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].table") }}{% endif -%}
{% if not src.natural_keys %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].natural_keys") }}{% endif -%}
{% if src.natural_keys is string %}{{ exceptions.raise_compiler_error("sources[" ~ loop.index0 ~ "].natural_keys must be a list") }}{% endif -%}
{% if not src.load_dts %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].load_dts") }}{% endif -%}
{% if not src.rec_src %}{{ exceptions.raise_compiler_error(msg ~ "sources[" ~ loop.index0 ~ "].rec_src") }}{% endif -%}
{% endfor -%}
{% endmacro -%}