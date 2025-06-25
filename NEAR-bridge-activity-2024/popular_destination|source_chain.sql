SELECT
  CASE
    WHEN direction = 'inbound' THEN source_chain
    WHEN direction = 'outbound' THEN destination_chain
  END AS chain,
  direction,
  COUNT(tx_hash) AS transaction_count
FROM
  near.defi.ez_bridge_activity
WHERE
  block_timestamp >= '{{start_date}}'
GROUP BY
  direction,
  chain
ORDER BY
  transaction_count DESC;
