-- SCD2 chain: each version ends exactly where the SKU's next version starts (no gap, no overlap),
-- every version is at least one day long, and the cost really changes between two versions (rule 6).
with chained as (
  select
    sku,
    valid_from,
    valid_to,
    unit_cost_sar,
    lead(valid_from) over (partition by sku order by valid_from)    as next_valid_from,
    lag(unit_cost_sar) over (partition by sku order by valid_from)  as previous_cost
  from {{ ref('product_cost_history') }}
)

select *
from chained
where valid_to is distinct from next_valid_from      -- a gap or an overlap
   or valid_to <= valid_from                         -- an empty or backwards version
   or unit_cost_sar = previous_cost                  -- a "new" version with the same cost
