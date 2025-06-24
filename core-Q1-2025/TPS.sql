SELECT
  MAX(txns) AS max_tps
FROM
  (
    SELECT
      DATE_TRUNC(second, block_timestamp) AS sec,
      COUNT(*) AS txns
    FROM
      core.core.fact_transactions
    WHERE
      block_timestamp >= '2025-01-01' --AND '2025-03-26'
    GROUP BY
      1
  ) subquery 
