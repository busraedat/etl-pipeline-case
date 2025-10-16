
SELECT

  (SELECT sd.active_subs
   FROM {{ ref('mart_subscription_daily') }} sd
   WHERE sd.date = (SELECT MAX(date) FROM {{ ref('mart_subscription_daily') }})
  ) AS active_subs,


  (SELECT COALESCE(SUM(o.net_revenue), 0)
   FROM {{ ref('fct_order') }} o
   WHERE UPPER(o.payment_status) = 'PAID'
     AND DATE(o.order_date) BETWEEN
         DATE_SUB((SELECT MAX(date) FROM {{ ref('mart_subscription_daily') }}), INTERVAL 30 DAY)
         AND (SELECT MAX(date) FROM {{ ref('mart_subscription_daily') }})
  ) AS mrr_try,


  SAFE_DIVIDE(
    (SELECT sd.cancelled_subs
     FROM {{ ref('mart_subscription_daily') }} sd
     WHERE sd.date = (SELECT MAX(date) FROM {{ ref('mart_subscription_daily') }})
    ),
    NULLIF((
      SELECT sd.active_subs
      FROM {{ ref('mart_subscription_daily') }} sd
      WHERE sd.date = DATE_SUB(
        (SELECT MAX(date) FROM {{ ref('mart_subscription_daily') }}), INTERVAL 1 DAY)
    ), 0)
  ) AS churn_rate,


  SAFE_DIVIDE(

    (SELECT mae.cac
     FROM {{ ref('mart_acquisition_efficiency') }} mae
     WHERE mae.date = (
       SELECT MAX(date)
       FROM {{ ref('mart_acquisition_efficiency') }}
       WHERE new_subs > 0)
    ),

    NULLIF(
      SAFE_DIVIDE(
        (SELECT COALESCE(SUM(o.net_revenue), 0)
         FROM {{ ref('fct_order') }} o
         WHERE UPPER(o.payment_status) = 'PAID'
           AND DATE(o.order_date) BETWEEN
               DATE_SUB((SELECT MAX(date) FROM {{ ref('mart_subscription_daily') }}), INTERVAL 30 DAY)
               AND (SELECT MAX(date) FROM {{ ref('mart_subscription_daily') }})
        ),
        30 * (SELECT sd.active_subs
              FROM {{ ref('mart_subscription_daily') }} sd
              WHERE sd.date = (SELECT MAX(date) FROM {{ ref('mart_subscription_daily') }}))
      ),
      0
    )
  ) AS cac_payback_days
