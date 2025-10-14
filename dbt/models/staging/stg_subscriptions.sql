select
  _id      as subscription_id,
  _user    as customer_id,
  status,
  createdAt as start_date
from {{ source('raw','subscriptions') }}
