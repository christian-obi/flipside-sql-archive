WITH categorized_transactions AS (
  SELECT
    from_address,
    to_address,
    origin_function_signature,
    COUNT(*) AS transaction_count,
    SUM(tx_fee_precise) AS total_tx_fee_used,
    SUM(cumulative_gas_used) AS total_cumulative_gas_used,
    CASE
      WHEN to_address = '0x2555223a15a931a71951707cb32a541f14e2c730'
      AND origin_function_signature = '0xbe0cdee6' THEN 'CVE interaction activity'
      WHEN to_address = '0xe6db1fb846a59e0780124b659358b6d2ccb45a81'
      AND origin_function_signature = '0xe8bbf5d7' THEN 'stakedETH market activity'
      WHEN to_address = '0x9e7ebd0f8255f3a910bc77fd006a410e9d54ee36'
      AND origin_function_signature = '0xe8bbf5d7' THEN 'shortBTC market activity'
      WHEN to_address = '0xcda16e9c25f429f4b01a87ff302ee7943f2d5015'
      AND origin_function_signature = '0xe8bbf5d7' THEN 'longBTC market activity'
      ELSE 'Uncategorized'
    END AS activity_type
  FROM
    monad.testnet.fact_transactions
  WHERE
    from_address = lower('{{Address}}') --lower ('{0x8b4f8DF21c989d5FDa5a24B7cdDf501A095D6aB8}')
  GROUP BY
    from_address,
    to_address,
    origin_function_signature
)
SELECT
  activity_type,
  SUM(transaction_count) AS total_transactions,
  SUM(total_tx_fee_used) AS total_tx_fee_used,
  SUM(total_cumulative_gas_used) AS total_cumulative_gas_used
FROM
  categorized_transactions
GROUP BY
  activity_type
