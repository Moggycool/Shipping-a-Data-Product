from sqlalchemy import text
from api.config import settings

SCHEMA = settings.MART_SCHEMA


def q_health():
    return text("select 1 as ok")


def q_search_messages():
    # channel_name filter works through dim_channels join
    return text(f"""
        select
            m.message_id,
            c.channel_name,
            m.message_ts::text as message_ts,
            m.message_text,
            m.message_length,
            m.views,
            m.forwards,
            m.reply_count,
            m.has_image
        from {SCHEMA}.fct_messages m
        join {SCHEMA}.dim_channels c
          on m.channel_key = c.channel_key
        where (:q is null or m.message_text ilike '%' || :q || '%')
          and (:channel_name is null or c.channel_name = :channel_name)
        order by m.message_ts desc nulls last
        limit :limit
        offset :offset
    """)


def q_channel_activity(grain: str):
    # grain injected only after validation in API layer (day|week|month)
    return text(f"""
        select
            date_trunc('{grain}', m.message_ts)::date::text as period_start,
            count(*)::int as message_count,
            avg(m.views)::float as avg_views
        from {SCHEMA}.fct_messages m
        join {SCHEMA}.dim_channels c
          on m.channel_key = c.channel_key
        where c.channel_name = :channel_name
          and m.message_ts >= (now() - (:days || ' days')::interval)
        group by 1
        order by 1
    """)


def q_top_products():
    return text(f"""
        with tokens as (
            select
                lower(regexp_replace(token, '[^a-z0-9]+', '', 'g')) as term
            from {SCHEMA}.fct_messages m,
                 regexp_split_to_table(coalesce(m.message_text,''), '\\s+') as token
        )
        select
            term,
            count(*)::int as mentions
        from tokens
        where term is not null
          and term <> ''
          and length(term) >= 3
          and term !~ '^[0-9]+$'
          and term not in (
              'the','and','for','with','from','this','that','you','your',
              'are','was','were','have','has','had','not','but','all','any',
              'now','new','get','our','out','use','per','price','birr','etb'
          )
        group by term
        order by mentions desc
        limit :limit
    """)


def q_visual_content():
    # Dedupe image_path so one image counts once even if multiple detections exist.
    # Expects fct_image_detections has channel_key + image_path + image_category.
    return text(f"""
        with img as (
            select distinct
                d.image_path,
                d.channel_key,
                d.image_category
            from {SCHEMA}.fct_image_detections d
        ),
        img_named as (
            select
                i.image_path,
                c.channel_name,
                i.image_category
            from img i
            join {SCHEMA}.dim_channels c
              on i.channel_key = c.channel_key
            where (:channel_name is null or c.channel_name = :channel_name)
        )
        select
            coalesce(image_category, 'other') as image_category,
            count(*)::int as n
        from img_named
        group by 1
        order by n desc
    """)


def q_visual_total_images():
    return text(f"""
        select count(*)::int as total_images
        from (
            select distinct d.image_path
            from {SCHEMA}.fct_image_detections d
            join {SCHEMA}.dim_channels c
              on d.channel_key = c.channel_key
            where (:channel_name is null or c.channel_name = :channel_name)
        ) x
    """)
