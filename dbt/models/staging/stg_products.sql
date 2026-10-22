-- One row per product. SKUs are trimmed (rule 2): two of them carry trailing spaces in the app database,
-- and the supplier files write them without, so an untrimmed join would never find their cost.
select
    product_id,
    trim(sku)                                               as sku,
    name_en,
    category
from {{ source('raw', 'products') }}
