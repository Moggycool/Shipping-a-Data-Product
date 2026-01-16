select *
from {{ ref('stg_telegram_messages') }}
where views < 0