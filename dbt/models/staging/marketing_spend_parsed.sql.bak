
select
  initcap(trim(Channel))                           as channel,
  parse_date('%Y_%m_%d', day_str)                  as date,
  safe_cast(replace(spend_txt, ',', '') as int64)  as spend_try
from {{ source('raw','MarketingSpend') }}
unpivot (
  spend_txt for day_str in (
    `2025_09_20`, `2025_09_21`, `2025_09_22`, `2025_09_23`,
    `2025_09_24`, `2025_09_25`, `2025_09_26`, `2025_09_27`
  )
)
