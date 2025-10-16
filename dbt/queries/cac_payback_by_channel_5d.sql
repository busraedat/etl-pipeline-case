SELECT
  ms.date,
  ms.channel,
  ms.spend_try,
  SAFE_MULTIPLY(subs.new_subs, SAFE_DIVIDE(ms.spend_try, NULLIF(tot.total_spend, 0))) AS new_subs_attributed,
  SAFE_DIVIDE(ms.spend_try, NULLIF(SAFE_MULTIPLY(subs.new_subs, SAFE_DIVIDE(ms.spend_try, NULLIF(tot.total_spend, 0))), 0)) AS cac_try,
  SAFE_DIVIDE(
    SAFE_DIVIDE(ms.spend_try, NULLIF(SAFE_MULTIPLY(subs.new_subs, SAFE_DIVIDE(ms.spend_try, NULLIF(tot.total_spend, 0))), 0)),
    NULLIF(arpd.revenue_per_user_per_day, 0)
  ) AS payback_days_est
FROM (
  SELECT date, channel, SUM(spend_try) AS spend_try
  FROM `ordinal-skyline-441507-p6.core.fct_marketing_spend`
  WHERE date BETWEEN DATE_SUB((SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.fct_marketing_spend`), INTERVAL 4 DAY)
                  AND        (SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.fct_marketing_spend`)
  GROUP BY date, channel
) AS ms
JOIN (
  SELECT date, SUM(spend_try) AS total_spend
  FROM `ordinal-skyline-441507-p6.core.fct_marketing_spend`
  WHERE date BETWEEN DATE_SUB((SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.fct_marketing_spend`), INTERVAL 4 DAY)
                  AND        (SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.fct_marketing_spend`)
  GROUP BY date
) AS tot
ON tot.date = ms.date
JOIN (
  SELECT date, SUM(new_subs) AS new_subs
  FROM `ordinal-skyline-441507-p6.core.mart_subscription_daily`
  WHERE date BETWEEN DATE_SUB((SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.fct_marketing_spend`), INTERVAL 4 DAY)
                  AND        (SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.fct_marketing_spend`)
  GROUP BY date
) AS subs
ON subs.date = ms.date
CROSS JOIN (
  SELECT SAFE_DIVIDE(
           (SELECT COALESCE(SUM(net_revenue),0)
            FROM `ordinal-skyline-441507-p6.core.fct_order`
            WHERE UPPER(payment_status)='PAID'
              AND DATE(order_date) BETWEEN DATE_SUB((SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.mart_subscription_daily`), INTERVAL 30 DAY)
                                       AND        (SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.mart_subscription_daily`)
           ),
           30 * NULLIF((SELECT active_subs
                        FROM `ordinal-skyline-441507-p6.core.mart_subscription_daily`
                        WHERE date=(SELECT MAX(date) FROM `ordinal-skyline-441507-p6.core.mart_subscription_daily`)
                       ),0)
         ) AS revenue_per_user_per_day
) AS arpd
ORDER BY ms.date, ms.channel;
