-- discount_total Ã¢â€°Â¥ 0
select *
from {{ ref('fct_order') }}
where discount_total < 0
