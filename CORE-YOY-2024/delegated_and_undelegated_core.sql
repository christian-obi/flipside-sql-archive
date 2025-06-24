--undelegated- lower('0x888585afd9421c43b48dc50229aa045dd1048c03602b46c83ad2aa36be798d42')
with extract as (
  SELECT
    block_timestamp,
    tx_hash,
    utils.udf_hex_to_int(substr(data, 3, 64)) / pow(10, 18) as core_amount,
    utils.udf_hex_to_int(substr(data, 67, 64)) / pow(10, 18) as stcore_amount
  FROM
    core.core.fact_event_logs
  where
    topic_0 = lower(
      '0x69e36aaf9558a3c30415c0a2bc6cb4c2d592c041a0718697bf69c2e7c7e0bdac'
    ) --delegatedcoin
    and contract_address in (
      '0xf5fa1728babc3f8d2a617397fac2696c958c3409',
      '0x0000000000000000000000000000000000001007'
    )
    and tx_succeeded
    and block_timestamp >= '2024-01-01'
)
SELECT
  count(DISTINCT tx_hash) as total_transactions,
  sum(core_amount) as cumulative_amount_delegated,
from
  extract
