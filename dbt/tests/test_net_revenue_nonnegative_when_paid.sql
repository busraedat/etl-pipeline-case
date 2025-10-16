-- net_revenue â‰¥ 0 yalnÄ±zca payment_status='paid' iken
select *
from {{ ref('fct_order') }}
where payment_status = 'paid'
  and net_revenue < 0
