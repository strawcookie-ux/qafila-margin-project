-- Reconciliation: the margin table must hold every eligible order line and all its revenue, counted
-- straight from staging. If a join silently dropped or duplicated lines, the two sides differ.
with source_lines as (
  select
    count(*)                  as lines,
    round(sum(i.revenue_sar), 2) as revenue_sar
  from {{ ref('stg_order_items') }} i
  join {{ ref('stg_orders') }} o
    on o.order_id = i.order_id
  where o.status = 'delivered'
    and i.quantity > 0
    and o.order_date >= date '{{ var("margin_start_date") }}'
),

margin as (
  select
    sum(lines)               as lines,
    round(sum(revenue_sar), 2) as revenue_sar
  from {{ ref('weekly_margin') }}
)

select *
from source_lines s, margin m
where s.lines <> m.lines
   or abs(s.revenue_sar - m.revenue_sar) > 1    -- per-row rounding may move the total by a few halalas
