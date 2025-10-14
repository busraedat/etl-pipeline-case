{{ config(materialized='table', schema='core') }}

select
  order_id,
  customer_id,
  subscription_id,
  order_date,
  items_total,
  discount_total,
  shipping_fee,

  case
    when coalesce(charged_amount, 0) > 0 then 'paid'
    else 'unpaid'
  end as payment_status,

  case
    when coalesce(charged_amount, 0) > 0 then
      coalesce(items_total, 0)
      - coalesce(discount_total, 0)
      + coalesce(shipping_fee, 0)
    else 0
  end as net_revenue

from {{ ref('orders_for_fact') }};
