{#
    Referential integrity check between two event streams where either stream may arrive up to
    `lookback_days` late across the start or end of the ingested window. Within the settled
    interior [min(event_date) + lookback_days .. max(event_date) - lookback_days], every child
    row must have a matching parent row.
#}
{% test settled_relationships(model, column_name, to, field, lookback_days=2) %}
    with bounds as (
        select
            date_add(min(event_date), interval {{ lookback_days }} day) as min_settled_date,
            date_sub(max(event_date), interval {{ lookback_days }} day) as max_settled_date
        from {{ to }}
    ),

    child as (
        select
            child_model.{{ column_name }} as from_field
        from {{ model }} as child_model
        cross join bounds
        where
            child_model.{{ column_name }} is not null
            and child_model.event_date between bounds.min_settled_date and bounds.max_settled_date
    ),

    parent as (
        select {{ field }} as to_field
        from {{ to }}
    )

    select child.from_field
    from child
    left join parent on child.from_field = parent.to_field
    where parent.to_field is null
{% endtest %}
