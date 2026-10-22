-- One row per row of every supplier file, with the reason it can't be used (NULL = usable).
-- The quarantine rule (rule 3) lives here and only here: product_cost_history reads the usable rows,
-- price_quarantine reads the others, so the two can never disagree.
select
    s.source_file,
    s.sku,
    s.valid_from,
    s.raw_cost,
    s.unit_cost_sar,
    case
        when p.sku is null                                      then 'unknown_sku'   -- checked first: an unknown SKU
        when s.unit_cost_sar is null or s.unit_cost_sar <= 0    then 'invalid_cost'  -- is rejected whatever its cost
    end                                                     as reject_reason
from {{ ref('stg_supplier_prices') }} s
left join {{ ref('stg_products') }} p
  on p.sku = s.sku                                          -- both sides already trimmed (rule 2)
