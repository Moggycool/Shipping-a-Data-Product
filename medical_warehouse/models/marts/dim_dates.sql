with bounds as (
    select
        min(message_ts::date) as min_date,
        max(message_ts::date) as max_date
    from {{ ref('stg_telegram_messages') }}
),
dates as (
    select generate_series(min_date, max_date, interval '1 day')::date as full_date
    from bounds
)
select
    to_char(full_date, 'YYYYMMDD')::int as date_key,
    full_date,
    extract(isodow from full_date)::int as day_of_week,
    trim(to_char(full_date, 'Day')) as day_name,
    extract(week from full_date)::int as week_of_year,
    extract(month from full_date)::int as month,
    trim(to_char(full_date, 'Month')) as month_name,
    extract(quarter from full_date)::int as quarter,
    extract(year from full_date)::int as year,
    (extract(isodow from full_date) in (6, 7)) as is_weekend
from dates

