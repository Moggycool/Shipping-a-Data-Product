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

    coalesce(m.views, 0)::int        as views,
    coalesce(m.forwards, 0)::int     as forwards,
    coalesce(m.reply_count, 0)::int  as reply_count,

    -- prefer a boolean from staging; otherwise derive from media flags/types
    coalesce(
        m.has_image,
        m.has_media,
        (m.media_type is not null),
        false
    ) as has_image

from msgs m
join channels c
  on m.channel_username = c.channel_name  -- change this line if your dim uses username instead
