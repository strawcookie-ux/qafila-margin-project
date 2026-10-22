-- Internal consistency of every margin row: the margin is costed revenue minus cost, costed revenue is
-- part of the revenue, and uncosted lines are part of the lines.
select *
from {{ ref('weekly_margin') }}
where abs(costed_revenue_sar - cost_sar - margin_sar) > 0.01
   or costed_revenue_sar > revenue_sar + 0.01
   or uncosted_lines > lines
   or uncosted_lines < 0
