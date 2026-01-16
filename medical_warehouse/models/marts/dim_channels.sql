with msgs as (
    select *
    from {{ ref('stg_telegram_messages') }}
),
agg as (
    select
        channel_username as channel_name,
        min(message_ts::date) as first_post_date,
        max(message_ts::date) as last_post_date,
        count(*) as total_posts,
        avg(views) as avg_views
    from msgs
    group by 1
)
select
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} as channel_key,
    channel_name,
    'Medical' as channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
from agg