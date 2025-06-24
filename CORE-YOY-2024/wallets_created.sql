WITH daily_transaction_counts AS (
  SELECT
    from_address AS address,
    DATE(block_timestamp) AS tx_date,
    COUNT(tx_hash) AS daily_tx_count
  FROM
    core.core.fact_transactions
  GROUP BY
    address,
    tx_date
),
filtered_addresses AS (
  SELECT
    from_address AS address,
    SUM(value_precise) AS total_value,
    COUNT(tx_hash) AS total_transactions,
    MIN(block_timestamp) AS first_transaction_date
  FROM
    core.core.fact_transactions
  WHERE
    YEAR(block_timestamp) = 2024
  GROUP BY
    from_address
),
bot_addresses AS (
  SELECT
    DISTINCT address
  FROM
    daily_transaction_counts
  WHERE
    daily_tx_count > 200
),
spam_addresses AS (
  SELECT
    DISTINCT address
  FROM
    filtered_addresses
  WHERE
    total_value < 1
),
valid_addresses AS (
  SELECT
    DISTINCT address
  FROM
    filtered_addresses
  WHERE
    address NOT IN (
      SELECT
        address
      FROM
        bot_addresses
    )
    AND address NOT IN (
      SELECT
        address
      FROM
        spam_addresses
    )
)
SELECT
  date_trunc('day', fa.first_transaction_date) as first_tx_date,
  count(va.address) as address,
  sum(count(va.address)) over (
    order by
      date_trunc('day', fa.first_transaction_date)
  ) as total_new_address
FROM
  valid_addresses va
  JOIN filtered_addresses fa ON va.address = fa.address
group by
  date_trunc('day', fa.first_transaction_date)
ORDER BY
  first_tx_date DESC
