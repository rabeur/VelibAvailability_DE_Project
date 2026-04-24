{#
    Cross-engine dispatch helpers so the same Gold models materialise on
    Postgres (local target `prod`) and BigQuery (cloud target
    `bigquery_cloud`). We branch on `target.type` rather than use
    `adapter.dispatch` to keep the call sites compact in the models.
#}

{%- macro numeric_cast(expr) -%}
    {%- if target.type == 'bigquery' -%}
        cast({{ expr }} as numeric)
    {%- else -%}
        ({{ expr }})::numeric
    {%- endif -%}
{%- endmacro -%}


{%- macro date_sub_days(date_expr, days) -%}
    {#-
        BigQuery does not support the `date - interval 'N day'` operator form
        used by Postgres. Wrap both dialects here so incremental lookback
        filters stay portable.
    -#}
    {%- if target.type == 'bigquery' -%}
        date_sub({{ date_expr }}, interval {{ days }} day)
    {%- else -%}
        ({{ date_expr }} - interval '{{ days }} day')
    {%- endif -%}
{%- endmacro -%}
