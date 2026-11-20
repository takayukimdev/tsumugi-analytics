{#
    prod: one dataset per layer (stg, int, marts), named exactly as configured.
    dev / ci: everything in the target dataset (e.g. dev_tsumugi, ci_<sha>), so a single
    expiring dataset can be created up front and dropped as a unit.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == "prod" and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
