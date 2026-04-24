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


{%- macro first_by_desc(pick_expr, order_expr) -%}
    {#-
        Return the first value of `pick_expr` ordered by `order_expr` desc.
        Postgres uses 1-based array subscripts; BigQuery needs `[offset(0)]`
        and refuses an unbounded array, hence the explicit `limit 1`.
    -#}
    {%- if target.type == 'bigquery' -%}
        array_agg({{ pick_expr }} order by {{ order_expr }} desc limit 1)[offset(0)]
    {%- else -%}
        (array_agg({{ pick_expr }} order by {{ order_expr }} desc))[1]
    {%- endif -%}
{%- endmacro -%}
