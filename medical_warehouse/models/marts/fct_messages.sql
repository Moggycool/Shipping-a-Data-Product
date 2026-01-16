with msgs as (
    select *
    from {{ ref('stg_telegram_messages') }}
),
channels as (
    select *
    from {{ ref('dim_channels') }}
)

select
    m.message_id,
    c.channel_key,
    to_char(m.message_ts::date, 'YYYYMMDD')::int as date_key,
    m.message_ts,
    m.message_text,
    length(coalesce(m.message_text, '')) as message_length,
    m.views,
    m.forwards,
    m.reply_count,
    m.has_image
from msgs m
join channels c
  on m.channel_username = c.channel_name