WITH current_price AS (
  SELECT
    AVG(price) AS price
  FROM
    solana.price.ez_prices_hourly
  WHERE
    token_address = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
    AND hour :: date = CURRENT_DATE
),
token_actions AS (
  SELECT
    mint_amount / POW(10, decimal) AS amount
  FROM
    solana.defi.fact_token_mint_actions
  WHERE
    mint = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
  UNION
  ALL
  SELECT
    - burn_amount / POW(10, decimal) AS amount
  FROM
    solana.defi.fact_token_burn_actions
  WHERE
    mint = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
),
latest_balances AS (
  SELECT
    account_address,
    balance,
    block_timestamp
  FROM
    (
      SELECT
        account_address,
        balance,
        block_timestamp,
        ROW_NUMBER() OVER (
          PARTITION BY account_address
          ORDER BY
            block_timestamp DESC
        ) AS rn
      FROM
        solana.core.fact_token_balances
      WHERE
        mint = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
    ) ranked
  WHERE
    rn = 1
    AND balance > 0
)
SELECT
  SUM(ta.amount) AS volume,
  SUM(ta.amount) * (
    SELECT
      price
    FROM
      current_price
  ) AS volume_usd,
  (
    SELECT
      COUNT(DISTINCT account_address)
    FROM
      latest_balances
  ) AS holders
FROM
  token_actions ta
