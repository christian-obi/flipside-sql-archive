-- top 100 holders of MPLX
WITH Latest_Transactions AS (
  SELECT
    owner,
    balance,
    ROW_NUMBER() OVER (
      PARTITION BY owner
      ORDER BY
        BLOCK_TIMESTAMP DESC
    ) AS rn
  FROM
    solana.core.fact_token_balances
  WHERE
    MINT = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
),
price AS (
  SELECT
    price
  FROM
    crosschain.price.ez_prices_hourly
  WHERE
    token_address = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
  ORDER BY
    hour DESC
  LIMIT
    1
)
SELECT
  owner,
  round(balance, 6) AS amount,
  to_varchar((amount * p.price), '999,999,999.00') AS amount_usd,
  concat(round(100 * amount / sum(amount) over(), 2), '%') AS percent
FROM
  Latest_Transactions
  cross JOIN price p
WHERE
  rn = 1
  AND amount > 0.000001
ORDER BY
  amount DESC
LIMIT
  100
