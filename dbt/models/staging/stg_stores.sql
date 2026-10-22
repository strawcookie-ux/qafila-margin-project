-- One row per dark store. The city of an order line comes from here (rule 10), not from the customer.
select
    store_id,
    city
from {{ source('raw', 'stores') }}
