-- D7 retention. Cohort = the user's first inference date (JST). Retained = at least one
-- request exactly 7 days later. Not eligible: cohorts younger than 7 days, and cohorts on the
-- first day of data (left-censored: those users were active before the data starts).
with daily_activity as (
    select distinct
        user_id,
        event_date
    from {{ ref('fct_inference_requests') }}
    where event_date >= date('2000-01-01')
),

cohorts as (
    select
        user_id,
        min(event_date) as cohort_date
    from daily_activity
    group by user_id
),

data_bounds as (
    select
        min(event_date) as first_date,
        max(event_date) as last_date
    from daily_activity
)

select
    cohorts.user_id,
    cohorts.cohort_date,
    (
        cohorts.cohort_date > data_bounds.first_date
        and date_add(cohorts.cohort_date, interval 7 day) <= data_bounds.last_date
    ) as is_d7_eligible,
    day7.user_id is not null as is_retained_d7
from cohorts
cross join data_bounds
left join daily_activity as day7
    on
        cohorts.user_id = day7.user_id
        and day7.event_date = date_add(cohorts.cohort_date, interval 7 day)
