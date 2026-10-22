-- One row per order. The app stores UTC as text; Qafila counts days in Riyadh time (UTC+3),
-- so an order at 22:30 UTC on 30 August is a 31 August order.
select
    order_id,
    store_id,
    status,
    created_at::timestamp                                   as created_at_utc,
    (created_at::timestamp + interval 3 hour)::date         as order_date
from {{ source('raw', 'orders') }}
