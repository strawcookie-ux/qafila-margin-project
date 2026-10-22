-- Gross margin: one row per ISO week x city x category that has at least one counted order line.
-- Uncosted lines count in lines and revenue_sar, but not in the cost columns (rule 9).
with weekly as (
    select
        weekofyear(order_date)::int                                         as iso_week,   -- ISO week (rule 10)
        city,
        category,
        count(*)::int                                                       as lines,
        round(sum(revenue_sar), 2)                                          as revenue_sar,
        round(coalesce(sum(revenue_sar) filter (where is_costed), 0), 2)    as costed_revenue_sar,
        round(coalesce(sum(cost_sar), 0), 2)                                as cost_sar,
        (count(*) filter (where not is_costed))::int                        as uncosted_lines
    from {{ ref('order_lines_costed') }}
    group by all
)

-- the margin from the rounded totals, so margin_sar = costed_revenue_sar - cost_sar holds to the halala
select
    iso_week,
    city,
    category,
    lines,
    revenue_sar,
    costed_revenue_sar,
    cost_sar,
    round(costed_revenue_sar - cost_sar, 2)                                 as margin_sar,
    uncosted_lines
from weekly
