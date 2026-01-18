with det as (
    select *
    from {{ ref('stg_yolo_detections') }}
),
msg as (
    select
        message_id,
        channel_key,
        date_key,
        views
    from {{ ref('fct_messages') }}
)
select
    det.message_id,
    msg.channel_key,
    msg.date_key,
    det.detected_class,
    det.confidence_score,
    det.image_category,
    det.image_path,
    det.model_name,
    det.inference_ts,
    msg.views
from det
left join msg
  on det.message_id = msg.message_id