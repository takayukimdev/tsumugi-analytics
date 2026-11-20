-- SCD2 org dimension. Join facts with
--   fact.org_id = dim.org_id and fact.event_ts >= dim.valid_from
--   and (dim.valid_to is null or fact.event_ts < dim.valid_to)
select
    concat(org_id, '@', format_timestamp('%Y%m%dT%H%M%E3S', valid_from)) as org_version_id,
    org_id,
    plan,
    is_zdr,
    change_type,
    valid_from,
    valid_to,
    valid_to is null as is_current
from {{ ref('int_org_history') }}
