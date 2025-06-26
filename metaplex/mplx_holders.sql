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
)
SELECT
  COUNT(*) AS "MPLX Holders"
FROM
  Latest_Transactions
WHERE
  rn = 1
  AND balance > 0;
