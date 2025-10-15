

select
  customer_id,
  min(signup_date)            as signup_date,  
  any_value(state_name)       as city,        
  any_value(district_name)    as district
from {{ ref('stg_address_city') }}
group by customer_id
