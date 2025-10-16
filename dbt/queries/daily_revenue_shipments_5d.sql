SELECT
  cal.d AS day,
  IFNULL(rev.revenue, 0)    AS revenue,
  IFNULL(sh.shipments, 0)   AS shipments
FROM (
  SELECT d
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_SUB((
        SELECT MAX(
          DATE(
            COALESCE(
              SAFE_CAST(NULLIF(o.createdAt,'') AS TIMESTAMP),
              SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', NULLIF(o.createdAt,'')),
              SAFE.PARSE_TIMESTAMP('%Y/%m/%d %H:%M:%E*S', NULLIF(o.createdAt,'')),
              SAFE.PARSE_TIMESTAMP('%Y-%m-%d', NULLIF(o.createdAt,'')),
              SAFE.PARSE_TIMESTAMP('%Y/%m/%d', NULLIF(o.createdAt,''))
            )
          )
        )
        FROM `ordinal-skyline-441507-p6.raw.orders` AS o
      ), INTERVAL 4 DAY),
      (
        SELECT MAX(
          DATE(
            COALESCE(
              SAFE_CAST(NULLIF(o.createdAt,'') AS TIMESTAMP),
              SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', NULLIF(o.createdAt,'')),
              SAFE.PARSE_TIMESTAMP('%Y/%m/%d %H:%M:%E*S', NULLIF(o.createdAt,'')),
              SAFE.PARSE_TIMESTAMP('%Y-%m-%d', NULLIF(o.createdAt,'')),
              SAFE.PARSE_TIMESTAMP('%Y/%m/%d', NULLIF(o.createdAt,''))
            )
          )
        )
        FROM `ordinal-skyline-441507-p6.raw.orders` AS o
      )
    )
  ) AS d
) AS cal
LEFT JOIN (
  SELECT
    DATE(
      COALESCE(
        SAFE_CAST(NULLIF(o.createdAt,'') AS TIMESTAMP),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', NULLIF(o.createdAt,'')),
        SAFE.PARSE_TIMESTAMP('%Y/%m/%d %H:%M:%E*S', NULLIF(o.createdAt,'')),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d', NULLIF(o.createdAt,'')),
        SAFE.PARSE_TIMESTAMP('%Y/%m/%d', NULLIF(o.createdAt,''))
      )
    ) AS d,
    SUM(
      SAFE_CAST(
        JSON_EXTRACT_SCALAR(REGEXP_REPLACE(o.price, r"'", '"'), '$.chargedAmount')
        AS NUMERIC
      )
    ) AS revenue
  FROM `ordinal-skyline-441507-p6.raw.orders` AS o
  GROUP BY d
) AS rev
  ON rev.d = cal.d
LEFT JOIN (
  SELECT
    DATE(
      COALESCE(
        SAFE_CAST(NULLIF(o.createdAt,'') AS TIMESTAMP),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', NULLIF(o.createdAt,'')),
        SAFE.PARSE_TIMESTAMP('%Y/%m/%d %H:%M:%E*S', NULLIF(o.createdAt,'')),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d', NULLIF(o.createdAt,'')),
        SAFE.PARSE_TIMESTAMP('%Y/%m/%d', NULLIF(o.createdAt,''))
      )
    ) AS d,
    COUNTIF(
      REGEXP_CONTAINS(UPPER(COALESCE(NULLIF(o.status,''),'UNKNOWN')), r'DELIVER')
    ) AS shipments
  FROM `ordinal-skyline-441507-p6.raw.orders` AS o
  GROUP BY d
) AS sh
  ON sh.d = cal.d
ORDER BY day;
