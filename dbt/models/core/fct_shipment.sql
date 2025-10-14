
select
  sp.shipment_id,
  sp.order_id,
  upper(trim(o.status)) as latest_status,
  case when upper(trim(o.status)) = 'DELIVERED' then sp.delivered_at end as delivered_at,
  sp.carrier
from {{ ref('shipments_parsed') }} as sp
left join {{ source('raw','orders') }} as o
  on o._id = sp.order_id
