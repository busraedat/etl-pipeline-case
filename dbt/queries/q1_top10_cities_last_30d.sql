SELECT
  t.city,
  COUNT(DISTINCT t.customer_id) AS active_subscribers_30d
FROM (
  SELECT
    s.customer_id,
    c.city,
    DATE(s.start_date) AS start_date,
    DATE(COALESCE(s.end_date, CURRENT_TIMESTAMP())) AS end_date
  FROM `ordinal-skyline-441507-p6.core.dim_subscription` AS s
  JOIN `ordinal-skyline-441507-p6.core.dim_customer` AS c
    ON c.customer_id = s.customer_id
) AS t
WHERE
  GREATEST(t.start_date, DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
  <=
  LEAST(t.end_date, CURRENT_DATE())
GROUP BY t.city
ORDER BY active_subscribers_30d DESC
LIMIT 10;
