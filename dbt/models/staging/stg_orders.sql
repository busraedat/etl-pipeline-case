select
  _id   as order_id,
  _user as customer_id
from {{ source('raw','orders') }}
