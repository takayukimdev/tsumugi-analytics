-- SCD2 history of each org's plan and ZDR setting, built from org_changed events.
-- Event-sourced rather than a dbt snapshot: history is reproducible on backfill and does not
-- depend on how often dbt happened to run.
with changes as (
    select
        org_id,
        json_value(properties, '$.change_type') as change_type,
        json_value(properties, '$.plan') as plan,
        cast(json_value(properties, '$.zdr') as bool) as is_zdr,
        timestamp(json_value(properties, '$.changed_at')) as changed_at
    from {{ ref('int_app_events_deduped') }}
    where event_name = 'org_changed'
)

select
    org_id,
    change_type,
    plan,
    is_zdr,
    changed_at as valid_from,
    lead(changed_at) over (partition by org_id order by changed_at) as valid_to
from changes
