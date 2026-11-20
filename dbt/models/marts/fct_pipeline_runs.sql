-- Operational observability mart: flattened dbt run_results joined to BigQuery
-- INFORMATION_SCHEMA.JOBS for authoritative billing, slot usage, and error codes.
-- On-demand Tokyo (asia-northeast1) price: $6.25 per TiB (1,099,511,627,776 bytes).
with run_results as (
    select *
    from {{ ref('stg_pipeline__dbt_run_results') }}
    qualify
        row_number() over (
            partition by run_node_id
            order by _loaded_at desc
        ) = 1
),

bq_jobs as (
    select
        job_id,
        total_bytes_billed,
        total_slot_ms,
        error_result.reason as bq_error_reason
    from `region-asia-northeast1`.INFORMATION_SCHEMA.JOBS  -- noqa: CP01, CP02, CP03
    where creation_time >= timestamp_sub(current_timestamp(), interval 30 day)
)

select
    run_results.run_node_id,
    run_results.target_date,
    run_results.step,
    run_results.invocation_id,
    run_results.unique_id,
    run_results.resource_type,
    run_results.status,
    run_results.execution_time_s,
    run_results.failures,
    run_results.job_id,
    coalesce(bq_jobs.total_bytes_billed, run_results.adapter_bytes_billed, 0)
        as total_bytes_billed,
    coalesce(bq_jobs.total_slot_ms, run_results.adapter_slot_ms, 0) as total_slot_ms,
    bq_jobs.bq_error_reason,
    cast(
        coalesce(bq_jobs.total_bytes_billed, run_results.adapter_bytes_billed, 0)
        * 6.25 / 1099511627776
        as numeric
    ) as estimated_cost_usd,
    run_results.executed_at,
    run_results._loaded_at
from run_results
left join bq_jobs on run_results.job_id = bq_jobs.job_id
