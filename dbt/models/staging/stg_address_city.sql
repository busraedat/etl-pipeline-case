
select
  a._user as customer_id,
  a._id   as address_id,

  cs.district_id     as district_id_ref,
  cs.district_id     as district_id,
  cs.district_name   as district_name,
  cs.state_name      as state_name,

  coalesce(up.signup_date, fo.first_order_date) as signup_date
from {{ source('raw','addresses') }} a


left join (
  select
    c._id  as district_id,
    c.name as district_name,
    s.name as state_name
  from {{ source('raw','cities') }}  c
  left join {{ source('raw','states') }} s
    on s._id = c._state
) cs
  on cs.district_id = a._city


left join (
  select
    u._id as user_id,
    date(
      coalesce(
        safe_cast(nullif(u.createdAt,'') as timestamp),
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', nullif(u.createdAt,'')),
        safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S',      nullif(u.createdAt,'')),
        case when regexp_contains(u.createdAt, r'^\d{13}$')
             then timestamp_millis(cast(u.createdAt as int64)) end,
        case when regexp_contains(u.createdAt, r'^\d{10}$')
             then timestamp_seconds(cast(u.createdAt as int64)) end
      )
    ) as signup_date
  from {{ source('raw','users') }} u
) up
  on up.user_id = a._user


left join (
  select
    o._user as customer_id,
    min(
      date(
        coalesce(
          safe_cast(nullif(o.createdAt,'') as timestamp),
          safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', nullif(o.createdAt,'')),
          safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S',      nullif(o.createdAt,''))
        )
      )
    ) as first_order_date
  from {{ source('raw','orders') }} o
  group by 1
) fo
  on fo.customer_id = a._user
