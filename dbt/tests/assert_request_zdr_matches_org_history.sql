-- Cross-source contract: the zdr flag stamped on each inference log must equal the org's
-- ZDR state at request time according to org_changed events. A mismatch means one producer
-- is wrong, and ZDR handling cannot be trusted.
select
    requests.request_id,
    requests.is_zdr as request_is_zdr,
    history.is_zdr as org_is_zdr
from {{ ref('int_inference_requests_deduped') }} as requests
inner join {{ ref('int_org_history') }} as history
    on
        requests.org_id = history.org_id
        and requests.event_ts >= history.valid_from
        and (history.valid_to is null or requests.event_ts < history.valid_to)
where requests.is_zdr != history.is_zdr
