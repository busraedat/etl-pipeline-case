
select
  s._id    as shipment_id,
  s._order as order_id,

  regexp_extract(s.label, r"'provider': '([^']+)'") as carrier,


  timestamp(
    datetime(
      safe_cast(trim(split(regexp_extract(s.details, r"'deliveryDate': datetime\.datetime\(([^)]*)\)"), ',')[offset(0)]) as int64),
      safe_cast(trim(split(regexp_extract(s.details, r"'deliveryDate': datetime\.datetime\(([^)]*)\)"), ',')[offset(1)]) as int64),
      safe_cast(trim(split(regexp_extract(s.details, r"'deliveryDate': datetime\.datetime\(([^)]*)\)"), ',')[offset(2)]) as int64),
      safe_cast(trim(split(regexp_extract(s.details, r"'deliveryDate': datetime\.datetime\(([^)]*)\)"), ',')[offset(3)]) as int64),
      safe_cast(trim(split(regexp_extract(s.details, r"'deliveryDate': datetime\.datetime\(([^)]*)\)"), ',')[offset(4)]) as int64),
      0
    )
  ) as delivered_at
from {{ source('raw','shipments') }} as s
