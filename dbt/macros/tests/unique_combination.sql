{#
    Fails if any combination of `columns` appears more than once.
    A tiny local replacement for dbt_utils.unique_combination_of_columns, so the project has
    no package dependencies and `dbt parse` works offline without `dbt deps`.
#}
{% test unique_combination(model, columns) %}
    select
        {{ columns | join(", ") }},
        count(*) as n_rows
    from {{ model }}
    group by {{ columns | join(", ") }}
    having count(*) > 1
{% endtest %}
