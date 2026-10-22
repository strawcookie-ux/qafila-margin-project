-- Every SKU in the history has exactly one current version (the one with an empty valid_to).
-- Returns the SKUs that break it: the test passes when this returns nothing.
select
  sku,
  count(*) filter (where is_current) as current_versions
from {{ ref('product_cost_history') }}
group by sku
having count(*) filter (where is_current) <> 1
    or count(*) filter (where is_current and valid_to is not null) > 0
