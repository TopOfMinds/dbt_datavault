# dbt_datavault by [TopOfMinds](https://topofminds.se/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A [DBT](https://docs.getdbt.com/docs/introduction) package with macros for creating data vault 2023 styled basics data vault object. The data vault style is based on the work of [Hans Hultgren](https://hanshultgren.wordpress.com/) and the worldwide data vault community. And the templates is based on TopOfMinds 10+ years of data vault implementation experience.

## Installation
Add this package to your `packages.yml` file. E.g:
```
packages:
  - git: "https://github.com/TopOfMinds/dbt_datavault.git"
```
And run `dbt deps`.

Example usage can be found [here](https://github.com/TopOfMinds/dbt_datavault_example).

## Reference
The data vault objects can often depend directly on source data. If the source data is to far awy from what is needed in the data vault templates then stage models can be created as a first step.

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
| sources | a list of meta data for each source table for the satellite | X |
|  name | if the source is a DBT source then it must have a name field (`source(name, table)`) |   |
|  table | the table part of a `source(name, table)` or a `ref(table)` | X |
|  natural_keys | a list of the natural keys for the hub key | X |
|  attributes | a list of the source fields that are used as input to the target attributes | X |
|  load_dts | the source column containing the ingestion time | X |
|  rec_src | a string describing the source | X |

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
