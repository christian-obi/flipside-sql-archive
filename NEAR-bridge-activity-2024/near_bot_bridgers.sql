WITH near_daily_activity AS (
  SELECT
    DATE(block_timestamp) AS activity_date,
    source_address,
    COUNT(tx_hash) AS daily_transactions
  FROM
    near.defi.ez_bridge_activity
  WHERE
    direction IN ('inbound', 'outbound')
    AND activity_date >= current_date -60
  GROUP BY
    activity_date,
    source_address
),
near_filtered_activity AS (
  SELECT
    source_address
  FROM
    near_daily_activity
  WHERE
    daily_transactions >= 50 --  addresses with more than 50 transactions per day as bots
),
near_active_bridgers AS (
  SELECT
    DISTINCT source_address
  FROM
    near.defi.ez_bridge_activity
  WHERE
    source_address IN (
      SELECT
        source_address
      FROM
        near_filtered_activity
    )
)
SELECT
  COUNT(DISTINCT source_address) AS Bot_bridgers_count
FROM
  near_active_bridgers
