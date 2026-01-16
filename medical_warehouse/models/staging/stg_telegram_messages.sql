with src as (
  select
    channel_username,
    message_id,
    channel_title,
    message_ts,
    message_ts::date as message_date,
    message_text,
    views,
    forwards,
    reply_count,
    has_media,
    has_image,
    media_type,
    media_path,
    partition_date,
    source_file,
    ingested_at,
    payload
  from raw.telegram_messages
)

select * from src