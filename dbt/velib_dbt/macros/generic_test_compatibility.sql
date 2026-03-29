{% macro test_accepted_values(model, column_name, values=None, quote=True, arguments=None) %}
    {#
      Compatibility wrapper for dbt versions that still call the generic test
      macro directly with top-level kwargs, while editor diagnostics already
      recommend the newer `arguments:` YAML shape.
    #}
    {% if arguments is not none %}
        {% set values = arguments.get('values', values) %}
        {% set quote = arguments.get('quote', quote) %}
{% endif %}

    with all_values as (
        select
            {{ column_name }} as value_field,
            count(*) as n_records
        from {{ model }}
        group by {{ column_name }}
    )

    select *
    from all_values
    where value_field not in (
        {% for value in values %}
    {% if quote %}
                '{{ value }}'
            {% else %}
        {{ value }}
    {% endif %}
    {% if not loop.last %},{% endif %}
{% endfor %}
    )
{% endmacro %}


{% macro test_relationships(model, column_name, to=None, field=None, arguments=None) %}
    {#
      Compatibility wrapper for the newer `arguments:` YAML shape used by
      relationships tests.
    #}
{% if arguments is not none %}
    {% set to = arguments.get('to', to) %}
    {% set field = arguments.get('field', field) %}
{% endif %}

    with child as (
        select {{ column_name }} as from_field
        from {{ model }}
        where {{ column_name }} is not null
    ),

    parent as (
        select {{ field }} as to_field
        from {{ to }}
    )

    select
        from_field
    from child
    left join parent
        on child.from_field = parent.to_field
    where parent.to_field is null
{% endmacro %}
