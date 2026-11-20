-- Dedupe BEFORE any merge: BigQuery MERGE fails if more than one source row matches the
-- same target row.
-- select * is intentional: dedupe passes every staging column through unchanged.
select *  -- noqa: AM04
from {{ ref('stg_api__inference_requests') }}
qualify row_number() over (partition by request_id order by _loaded_at desc) = 1
