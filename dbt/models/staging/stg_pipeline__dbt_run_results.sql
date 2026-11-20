with source as (
    select * from {{ source('pipeline', 'dbt_run_results') }}
)

select
    concat(invocation_id, ':', unique_id) as run_node_id,
    target_date,
    step,
    invocation_id,
    unique_id,
    resource_type,
    status,
    execution_time_s,
    failures,
    job_id,
    bytes_processed as adapter_bytes_processed,
    bytes_billed as adapter_bytes_billed,
    slot_ms as adapter_slot_ms,
    max_loaded_at,
    age_s,
    executed_at,
    _loaded_at
from source
