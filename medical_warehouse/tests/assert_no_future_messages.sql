select *
from {{ ref('stg_telegram_messages') }}
where message_ts > now()