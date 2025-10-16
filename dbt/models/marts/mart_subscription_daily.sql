
SELECT
  d AS date,
  -- o gÃƒÂ¼n baÃ…Å¸layanlar
  SUM(CASE WHEN DATE(s.start_date) = d THEN 1 ELSE 0 END) AS new_subs,
  -- o gÃƒÂ¼n iptaller
  SUM(CASE WHEN s.status = 'CANCELLED' AND DATE(s.end_date) = d THEN 1 ELSE 0 END) AS cancelled_subs,
  -- o gÃƒÂ¼n aktif aboneler
  SUM(
    CASE
      WHEN DATE(s.start_date) <= d AND (s.end_date IS NULL OR DATE(s.end_date) > d)
      THEN 1 ELSE 0
    END
  ) AS active_subs
FROM
  UNNEST(GENERATE_DATE_ARRAY(
    (SELECT MIN(DATE(start_date)) FROM {{ ref('dim_subscription') }}),
    (SELECT MAX(COALESCE(DATE(end_date), DATE(start_date))) FROM {{ ref('dim_subscription') }})
  )) AS d
LEFT JOIN {{ ref('dim_subscription') }} s
  ON TRUE
GROUP BY d
ORDER BY d
