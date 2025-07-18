WITH filtered_prices AS (
  SELECT
    HOUR,
    PRICE
  FROM
    solana.price.ez_prices_hourly
  WHERE
    TOKEN_ADDRESS = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
)
SELECT
  --DATE_TRUNC('day', HOUR) AS "Day",
  MIN(PRICE) AS "Minimum($) Price",
  MAX(PRICE) AS "Maximum($) Price"
FROM
  filtered_prices
