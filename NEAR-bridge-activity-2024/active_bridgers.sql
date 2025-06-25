WITH daily_activity AS (
  SELECT
    DATE(block_timestamp) AS activity_date,
    source_address,
    COUNT(tx_hash) AS daily_transactions
  FROM
    near.defi.ez_bridge_activity
  WHERE
    direction in ('inbound', 'outbound') -- Filter for inbound transactions
  GROUP BY
    activity_date,
    source_address
),
filtered_activity AS (
  SELECT
    source_address
  FROM
    daily_activity
  WHERE
    daily_transactions <= 50 -- Exclude addresses with more than 50 transactions per day
),
active_bridgers AS (
  SELECT
    DISTINCT source_address
  FROM
    near.defi.ez_bridge_activity
  WHERE
    source_address IN (
      SELECT
        source_address
      FROM
        filtered_activity
    )
)
SELECT
  COUNT(DISTINCT source_address) AS active_bridgers_count
FROM
  active_bridgers
