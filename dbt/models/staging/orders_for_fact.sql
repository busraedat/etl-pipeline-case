
select
 
  o._id   as order_id,
  o._user as customer_id,

 
  regexp_extract(
    ifnull(o.subscriptions, ''),
    r"ObjectId\('([0-9a-fA-F]{24})'\)"
  ) as subscription_id,

  date(
    coalesce(
      safe_cast(nullif(o.createdAt,'') as timestamp),
      safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S', nullif(o.createdAt,''))
    )
  ) as order_date,

  
  safe_cast(json_extract_scalar(replace(o.price, '''', '"'), '$.grossOriginalAmount')       as numeric) as items_total,
  safe_cast(json_extract_scalar(replace(o.price, '''', '"'), '$.grossPromoDiscountAmount')  as numeric) as discount_total,
  safe_cast(json_extract_scalar(replace(o.price, '''', '"'), '$.shipmentFeeAmount')         as numeric) as shipping_fee,
  safe_cast(json_extract_scalar(replace(o.price, '''', '"'), '$.chargedAmount')             as numeric) as charged_amount,


  coalesce(nullif(o.status,''), 'UNKNOWN') as src_payment_status

from {{ source('raw','orders') }} as o
