{#
    Override the default `generate_schema_name` so the staging layer does
    not require a separate BigQuery dataset.

    Default dbt behaviour concatenates `<default_schema>_<custom_schema>`,
    which on BigQuery means a brand-new `velib_gold_staging` dataset gets
    created on first run. The pipeline service account is intentionally
    scoped to dataEditor on `velib_silver` and `velib_gold` only, so a
    `bigquery.datasets.create` call is rejected.

    Postgres keeps the default split (gold / gold_staging) since schemas
    are cheap there and the separation has historical value. BigQuery
    materialises staging views directly inside the target dataset; the
    `stg_` prefix on every staging model already prevents name collisions
    with Gold tables (dim_*, fact_*, mart_*).
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if target.type == 'bigquery' -%}
        {{ default_schema }}
    {%- elif custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
