
SELECT
  m.date,
  SUM(m.spend_try) AS spend_try,
  SUM(s.new_subs)  AS new_subs,
  SAFE_DIVIDE(SUM(m.spend_try), NULLIF(SUM(s.new_subs), 0)) AS cac
FROM {{ ref('fct_marketing_spend') }} m
LEFT JOIN {{ ref('mart_subscription_daily') }} s
  ON s.date = m.date
GROUP BY m.date
ORDER BY m.date
