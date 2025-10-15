
SELECT
  d AS date,


  COALESCE(SUM(CASE WHEN UPPER(o.payment_status) = 'PAID' THEN o.net_revenue END), 0) AS daily_revenue,


  COUNT(DISTINCT CASE
                   WHEN UPPER(s.latest_status) = 'DELIVERED' THEN s.order_id
                 END) AS delivered_orders
FROM
  UNNEST(GENERATE_DATE_ARRAY(
    LEAST(
      (SELECT MIN(DATE(order_date))   FROM {{ ref('fct_order') }}),
      (SELECT MIN(DATE(delivered_at)) FROM {{ ref('fct_shipment') }})
    ),
    GREATEST(
      (SELECT MAX(DATE(order_date))   FROM {{ ref('fct_order') }}),
      (SELECT MAX(DATE(delivered_at)) FROM {{ ref('fct_shipment') }})
    )
  )) AS d
LEFT JOIN {{ ref('fct_order') }}    o ON DATE(o.order_date)   = d
LEFT JOIN {{ ref('fct_shipment') }} s ON DATE(s.delivered_at) = d
GROUP BY d
ORDER BY d
