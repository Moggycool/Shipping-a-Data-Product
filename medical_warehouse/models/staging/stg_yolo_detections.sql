with src as (
    select
        image_path,
        channel_name,
        cast(message_id as bigint) as message_id,
        detected_class,
        cast(confidence_score as double precision) as confidence_score,
        cast(bbox_x1 as double precision) as bbox_x1,
        cast(bbox_y1 as double precision) as bbox_y1,
        cast(bbox_x2 as double precision) as bbox_x2,
        cast(bbox_y2 as double precision) as bbox_y2,
        image_category,
        model_name,
        cast(inference_ts as timestamptz) as inference_ts
    from {{ ref('yolo_detections') }}
)
select * from src