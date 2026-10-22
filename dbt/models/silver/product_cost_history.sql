-- SCD2 history of supplier costs: one row per SKU per cost version.
-- valid_from is inclusive, valid_to exclusive (NULL for the current version).
with usable as (
    -- rule 3: quarantined rows are ignored everywhere else
    select sku, valid_from, unit_cost_sar, source_file
    from {{ ref('supplier_price_checks') }}
    where reject_reason is null
),

quotes as (
    -- rule 5: the same SKU and valid_from in several files -> the file whose name sorts last wins.
    -- That's how prices_2026-W39_fix.csv replaces prices_2026-W39.csv, without naming either file.
    select sku, valid_from, unit_cost_sar, source_file
    from usable
    qualify row_number() over (partition by sku, valid_from order by source_file desc) = 1
),

changes as (
    -- rule 6: compare every quote with the SKU's previous quote. A week where the SKU isn't quoted has
    -- no row at all, so the previous cost simply carries on (rule 4) and the gap never starts a version.
    select
        sku,
        valid_from,
        unit_cost_sar,
        source_file,
        lag(unit_cost_sar) over (partition by sku order by valid_from) as previous_cost
    from quotes
),

versions as (
    -- a new version only when the cost changes: the first quote of a SKU, or a different cost
    select sku, valid_from, unit_cost_sar, source_file
    from changes
    where previous_cost is null
       or unit_cost_sar <> previous_cost
)

select
    sku,
    unit_cost_sar,
    valid_from,
    lead(valid_from) over w                                 as valid_to,      -- where the next version starts
    lead(valid_from) over w is null                         as is_current,
    source_file                                                               -- lineage: the file that set this cost
from versions
window w as (partition by sku order by valid_from)
