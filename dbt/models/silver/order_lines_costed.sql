-- One row per counted order line (rule 7), with the supplier cost valid on its order date.
-- The grain of the margin: gold.weekly_margin only adds these rows up.
with lines as (
    select
        i.order_id,
        i.line_no,
        o.order_date,
        s.city,                                             -- rule 10: city from the order's store
        p.category,                                         -- ...and category from the product
        p.sku,
        i.quantity,
        i.revenue_sar                                       -- rule 8, computed in staging
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }} o
      on o.order_id = i.order_id
    -- left joins: a line whose store or product is missing still counts in lines and revenue
    left join {{ ref('stg_stores') }} s
      on s.store_id = o.store_id
    left join {{ ref('stg_products') }} p
      on p.product_id = i.product_id
    where o.status = 'delivered'
      and i.quantity > 0
      and o.order_date >= date '{{ var("margin_start_date") }}'
)

select
    l.*,
    h.unit_cost_sar,
    h.valid_from                                            as cost_valid_from,
    l.quantity * h.unit_cost_sar                            as cost_sar,      -- rule 9 (NULL when uncosted)
    h.sku is not null                                       as is_costed
from lines l
-- rule 9, the point-in-time join: the version where valid_from <= order date < valid_to.
-- Versions of a SKU never overlap, so a line matches at most one; a line with no match (SKU never quoted,
-- or ordered before its first quote) stays in with a NULL cost: an uncosted line.
left join {{ ref('product_cost_history') }} h
  on h.sku = l.sku
 and l.order_date >= h.valid_from
 and (l.order_date < h.valid_to or h.valid_to is null)
