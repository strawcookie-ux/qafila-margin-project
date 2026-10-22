-- One row per rejected supplier row, with the cost exactly as it appeared in the file.
-- This is the list that goes back to the supplier. The rule itself is in silver.supplier_price_checks.
select
    source_file,
    sku,
    valid_from,
    raw_cost,
    reject_reason                                           as reason      -- 'unknown_sku' or 'invalid_cost'
from {{ ref('supplier_price_checks') }}
where reject_reason is not null
