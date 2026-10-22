-- No supplier row may disappear: every row of every file is either usable or in the quarantine.
select *
from (
  select
    (select count(*) from {{ ref('stg_supplier_prices') }})                                       as file_rows,
    (select count(*) from {{ ref('supplier_price_checks') }} where reject_reason is null)         as usable_rows,
    (select count(*) from {{ ref('price_quarantine') }})                                          as quarantined_rows
)
where file_rows <> usable_rows + quarantined_rows
