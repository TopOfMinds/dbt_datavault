# dbt_datavault by [TopOfMinds](https://topofminds.se/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A [DBT](https://docs.getdbt.com/docs/introduction) package with macros for creating Data vault 2023 basics Data vault object. Data vault is based on the ever evolving work of [Hans Hultgren](https://hanshultgren.wordpress.com/) and the worldwide Data vault community. And the templates is based on Top of Minds 10+ years of Data vault implementation experience.

## Installation
Add this package to your `packages.yml` file. E.g:
```
packages:
  - git: "https://github.com/TopOfMinds/dbt_datavault.git"
```
And run `dbt deps`.

Example usage can be found [here](https://github.com/TopOfMinds/dbt_datavault_example).

## Reference
The Data vault objects can often depend directly on source data. If the source data is to far away from what is needed in the Data vault templates then stage models can be created as a first step.

### Hub

| key | value description | mandatory |
| --- | --- | --- |
| target | describes columnes in the generated data vault object | X |
|  hub_key | the name of the hub key | X |
|  natural_keys | a list of natural keys for the hub | X |
| sources | a list of meta data for each source table for the hub | X |
|  name | if the source is a DBT source then it must have a name field (`source(name, table)`) |   |
|  table | the table part of a `source(name, table)` or a `ref(table)` | X |
|  natural_keys | a list with the fields that are used as sources for the target natural keys | X |
|  load_dts | the source column containing the ingestion time | X |
|  rec_src | a string describing the source | X |
|  filter | source filter condition |  |

Example:
```
{% set metadata_yaml -%}
target: 
  hub_key: customer_key
  natural_keys: ['customer_id']
sources:
  - name: datalake
    table: sales
    natural_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.sales
    filter: 'customer_id IS NOT NULL'
  - name: datalake
    table: customer
    natural_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.customer
  - name: datalake
    table: customer_address
    natural_keys: ['customer_id']
    load_dts: ingestion_time
    rec_src: datalake.custommer_address
{%- endset %}

{{- dbt_datavault.hub(metadata_yaml) }}
```

### Link

| key | value description | mandatory |
| --- | --- | --- |
| target | describes columnes in the generated data vault object | X |
|  link_key | the name of the link top key | X |
|  hub_keys | a list of hub keys for the link | X |
| sources | a list of meta data for each source table for the link | X |
|  name | if the source is a DBT source then it must have a name field (`source(name, table)`) |   |
|  table | the table part of a `source(name, table)` or a `ref(table)` | X |
|  hub_natural_keys | a list of lists of the natural keys for each hub key | X |
|  load_dts | the source column containing the ingestion time | X |
|  rec_src | a string describing the source | X |
|  filter | source filter condition |  |

Example:
```
{% set metadata_yaml -%}
target: 
  link_key: sale_line_l_key
  hub_keys: ['sale_line_key', 'sale_key', 'product_key']
sources:
  - table: sales_line_stg
    hub_natural_keys: [['sales_line_id'], ['transaction_id'], ['product_id']]
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.link(metadata_yaml) }}
```

### Satellite

| key | value description | mandatory |
| --- | --- | --- |
| target | describes columnes in the generated data vault object | X |
|  hub_key | the name of the hub key | X |
|  attributes | a list of the attributes of the satellite | X |
|  no_deduplication | default `false`. if `true` removes the deduplication on target  |  |
|  deduplication_include | a list of fields to be included in deduplication. Overrides normal behavior where all fields will be added.  |  |
| sources | a list of meta data for each source table for the satellite | X |
|  name | if the source is a DBT source then it must have a name field (`source(name, table)`) |   |
|  table | the table part of a `source(name, table)` or a `ref(table)` | X |
|  natural_keys | a list of the natural keys for the hub key | X |
|  attributes | a list of the source fields that are used as input to the target attributes | X |
|  load_dts | the source column containing the ingestion time | X |
|  rec_src | a string describing the source | X |
|  filter | source filter condition |  |

Example:
```
{% set metadata_yaml -%}
target: 
  hub_key: sale_key
  attributes: ['effective_ts', 'amount', 'tax']
sources:
  - name: datalake
    table: sales
    natural_keys: [transaction_id]
    attributes: ['date', 'CAST(total AS numeric)', 'CAST(tax AS numeric)']
    load_dts: ingestion_time
    rec_src: datalake.sales
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
```

### Point In Time

| key | value description | mandatory |
| --- | --- | --- |
| target | describes columnes in the generated data vault object | X |
|  hub_key | the name of the hub key | X |
|  effective_ts | the effective timestamp for the point in time row | X |
| sources | a list of meta data for each source table for the point in time | X |
|  name | if the source is a DBT source then it must have a name field (`source(name, table)`) |   |
|  table | the table part of a `source(name, table)` or a `ref(table)` | X |
|  hub_key | the hub key for a satellite that is used in the point in time | X |
|  load_dts | the ingestion time for the satellite row | X |
|  effective_ts | the effective timestamp for the satellite row | X |

Example:
```
{% set metadata_yaml -%}
target: 
  hub_key: 'customer_key'
  effective_ts: 'effective_ts'
sources:
  - table: 'customer_main_s'
    hub_key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
  - table: 'customer_address_s'
    hub_key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
  - table: 'customer_class_sb'
    hub_key: 'customer_key'
    load_dts: 'load_dts'
    effective_ts: 'effective_ts'
{%- endset %}

{{- dbt_datavault.pit(metadata_yaml) }}

```

### Driving key satellite
The driving key satellite tracks changes on a relationship, link, and allows to interpret the many-to-many implementation of the Data vault links as a many-to-one relationship. This is done by end-dating relationships if a new relationship is created. The `driving_key` is the key of the relationship that will remain constant and the `other_key` is the key that will change over time.

To be able to use this pattern an `effective_link_satellite` must have been implemented on the relationship.

This can be done using the `satellite` macro provided in the package.

Example:
```
{% set metadata_yaml -%}
target: 
  hub_key: customer_customer_class_l_key
  attributes: ['effective_ts']
sources:
  - name: datalake
    table: customer_segmentations
    natural_keys: ['customer_id', 'customer_class_id']
    attributes: ['date']
    load_dts: ingestion_time
    rec_src: datalake.customer_segmentations
{%- endset %}

{{- dbt_datavault.satellite(metadata_yaml) }}
```

Next step will be to generate the driving key satellite on top of the `effective_link_satellite`:

| key | value description | mandatory |
| --- | --- | --- |
| target | describes columnes in the generated data vault object | X |
|  link_key | the name of the link key | X |
|  effective_ts | the effective timestamp | X |
| link_source | metadata for the link in the driving key satellite | X |
|  table | the link the driving key satellite relates to `ref(table)` | X |
|  link_key | the name of the link key | X |
|  driving_key | the key that drives the relationship n-1 | X |
|  other_key | the key that will change over time | X |
| sat_source | metadata for the effective_link_satellite | X |
|  table | the source effective_link_satellite  `ref(table)` | X |
|  link_key | the name of the link key | X |
|  effective_ts | the effective timestamp column of the satelitte | X |

```
{% set metadata_yaml -%}
target: 
  link_key: customer_customer_class_l_key
  effective_ts: effective_ts
link_source:
    table: customer_customer_class_l
    link_key: customer_customer_class_l_key
    driving_key: customer_key
    other_key: customer_class_key
sat_source:
    table: customer_customer_class_l_s
    link_key: customer_customer_class_l_key
    effective_ts: effective_ts
{%- endset %}

{{- dbt_datavault.driving_key_satellite(metadata_yaml) }}
```