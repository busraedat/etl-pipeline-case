-- discount_total ≥ 0
select *
from {{ ref('fct_order') }}
where discount_total < 0
