with base_daily_activity AS (
  SELECT
    DATE(block_timestamp) AS activity_date,
    origin_from_address,
    COUNT(tx_hash) AS daily_transactions
  FROM
    base.defi.ez_bridge_activity
  WHERE
    activity_date >= current_date -60
  GROUP BY
    activity_date,
    origin_from_address
),
base_filtered_activity AS (
  SELECT
    origin_from_address
  FROM
    base_daily_activity
  WHERE
    daily_transactions <= 50 -- Exclude addresses with more than 50 transactions per day as bots
),
base_active_bridgers AS (
  SELECT
    DISTINCT origin_from_address
  FROM
    base.defi.ez_bridge_activity
  WHERE
    origin_from_address IN (
      SELECT
        origin_from_address
      FROM
        base_filtered_activity
    )
)
SELECT
  COUNT(DISTINCT b.origin_from_address) AS base_active_bridgers_count
FROM
  base_active_bridgers b
