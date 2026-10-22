-- No zero or negative cost may reach the history: those rows belong in the quarantine (rule 3).
select *
from {{ ref('product_cost_history') }}
where unit_cost_sar <= 0
