-- Duplicate deliveries share event_id. Keep the most recently loaded copy so a re-delivery
-- flows into incremental marts as an update, never as a second row.
-- select * is intentional: dedupe passes every staging column through unchanged.
select *  -- noqa: AM04
from {{ ref('stg_app__events') }}
qualify row_number() over (partition by event_id order by _loaded_at desc) = 1
