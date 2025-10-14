select
  _id as customer_id
from {{ source('raw','users') }}
