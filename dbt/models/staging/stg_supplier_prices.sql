-- One row per row of every supplier file: the cost parsed into a number, nothing filtered yet.
-- Deciding which rows are usable is the job of silver (history) and quality (quarantine).
with src as (
    select
        source_file,
        sku,
        valid_from,
        -- rule 1: the cost is in unit_cost, or in cost_sar from week 39 on (a file only ever has one of them)
        coalesce(cost_sar, unit_cost)                       as raw_cost
    from {{ source('raw', 'supplier_prices') }}
)

select
    source_file,
    trim(sku)                                               as sku,         -- rule 2: compare trimmed SKUs
    try_cast(trim(valid_from) as date)                      as valid_from,
    raw_cost,                                                               -- kept exactly as written, for the quarantine
    -- rule 1: '9,49' and '٩٫٤٩' both mean 9.49. Arabic-Indic digits become 0-9, and both the Arabic decimal
    -- separator (٫) and the decimal comma become a dot. try_cast gives NULL instead of failing on anything
    -- that still isn't a number, and NULL is quarantined as an invalid cost. Rounded to 2 decimals (rule 6).
    round(try_cast(translate(trim(raw_cost), '٠١٢٣٤٥٦٧٨٩٫,', '0123456789..') as double), 2) as unit_cost_sar
from src
