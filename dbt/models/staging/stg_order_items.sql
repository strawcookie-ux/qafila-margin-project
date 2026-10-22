-- One row per order line (order_id, line_no), with its revenue (rule 8).
-- Lines with quantity <= 0 are kept here: staging cleans, it doesn't decide. The silver model filters them.
select
    order_id,
    line_no,
    product_id,
    quantity,
    unit_price,
    discount,
    round(quantity * unit_price - discount, 2)              as revenue_sar
from {{ source('raw', 'order_items') }}
