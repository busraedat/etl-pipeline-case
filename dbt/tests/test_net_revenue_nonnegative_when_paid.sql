-- net_revenue Ã¢â€°Â¥ 0 yalnÃ„Â±zca payment_status='paid' iken
select *
from {{ ref('fct_order') }}
where payment_status = 'paid'
  and net_revenue < 0
