-- Daily time spine required by MetricFlow (configured via `time_spine:` in YAML).
select date_day
from unnest(generate_date_array(date('2024-01-01'), date('2027-12-31'))) as date_day
