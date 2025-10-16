

select
  subscription_id,
  customer_id,

  case
    when next_order_date is null
      or next_order_date > current_timestamp() then 'ACTIVE'
    else 'CANCELLED'
  end as status,

  start_date,

  case
    when next_order_date <= current_timestamp() then next_order_date
  end as end_date

from {{ ref('stg_subscriptions_parsed') }}
