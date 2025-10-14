-- net_revenue ≥ 0 yalnızca payment_status='paid' iken
select *
from {{ ref('fct_order') }}
where payment_status = 'paid'
  and net_revenue < 0
