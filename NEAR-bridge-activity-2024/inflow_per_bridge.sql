WITH prices AS (
  SELECT
    date_trunc('day', hour) AS date,
    symbol,
    AVG(price) AS price
  FROM
    near.price.ez_prices_hourly
  WHERE
    symbol IN (
      'NEAR',
      'WETH',
      'WNEAR',
      'WBTC',
      'USDC.e',
      'USDT.e',
      'DAI',
      'AURORA',
      'STNEAR',
      'USDC',
      'USDT',
      'SOL',
      'LINEAR'
    )
    AND is_deprecated = FALSE
    AND blockchain ILIKE 'near protocol'
  GROUP BY
    1,
    2
),
wormhole_table AS (
  SELECT
    block_id,
    block_timestamp,
    tx_hash,
    token_address,
    amount_unadj,
    amount_adj,
    CASE
      WHEN token_address IN (
        '4.contract.portalbridge.near',
        '10.contract.portalbridge.near',
        '18.contract.portalbridge.near'
      ) THEN 'USDT'
      WHEN token_address IN (
        '36.contract.portalbridge.near',
        '15.contract.portalbridge.near'
      ) THEN 'WBTC'
      WHEN token_address IN (
        '26.contract.portalbridge.near',
        '24.contract.portalbridge.near',
        '1.contract.portalbridge.near'
      ) THEN 'WETH'
      WHEN token_address IN ('22.contract.portalbridge.near') THEN 'SOL'
      WHEN token_address IN (
        '37.contract.portalbridge.near',
        '40.contract.portalbridge.near',
        '3.contract.portalbridge.near',
        '41.contract.portalbridge.near',
        '16.contract.portalbridge.near',
        '39.contract.portalbridge.near',
        '38.contract.portalbridge.near',
        '7.contract.portalbridge.near',
        '12.contract.portalbridge.near'
      ) THEN 'USDC'
      WHEN token_address IN ('23.contract.portalbridge.near') THEN 'AURORA'
      WHEN token_address IN ('28.contract.portalbridge.near') THEN 'NEAR'
      ELSE symbol
    END AS symbol,
    CASE
      WHEN symbol = 'AURORA' THEN (amount_adj / POW(10, 18))
      WHEN symbol = 'WBTC' THEN (amount_adj / POW(10, 8))
      WHEN symbol = 'USDC' THEN (amount_adj / POW(10, 6))
      WHEN symbol = 'USDC.e' THEN (amount_adj / POW(10, 6))
      WHEN symbol = 'USDT.e' THEN (amount_adj / POW(10, 6))
      WHEN symbol = 'WETH' THEN (amount_adj / POW(10, 18))
      WHEN symbol = 'DAI' THEN (amount_adj / POW(10, 18))
      WHEN symbol = 'STNEAR' THEN (amount_adj / POW(10, 24))
      WHEN symbol = 'LINEAR' THEN (amount_adj / POW(10, 24))
      WHEN symbol = 'SOL' THEN (amount_adj / POW(10, 8))
      WHEN symbol = 'USDT' THEN (amount_adj / POW(10, 6))
      WHEN symbol = 'NEAR' THEN (amount_adj / POW(10, 8))
      ELSE amount
    END AS amount,
    amount_usd,
    destination_address,
    source_address,
    platform,
    bridge_address,
    destination_chain,
    source_chain,
    method_name,
    direction,
    receipt_succeeded,
    ez_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp,
  FROM
    near.defi.ez_bridge_activity
  WHERE
    platform = 'wormhole'
),
table1 as (
  SELECT
    *
  FROM
    near.defi.ez_bridge_activity
  UNION
  SELECT
    *
  from
    wormhole_table
),
amount_in_usd as (
  SELECT
    date_trunc('day', ea.block_timestamp) as tx_date,
    p.symbol as symbols,
    p.price as price,
    amount,
    (coalesce(amount, 0) * coalesce(price, 0)) as amount_usd,
    platform,
    direction,
    receipt_succeeded,
    source_chain,
    destination_chain,
    bridge_address,
    destination_address,
    source_address,
    tx_hash,
    token_address
  FROM
    table1 ea
    JOIN prices p on p.symbol = ea.symbol
    and p.date = date_trunc('day', ea.block_timestamp)
)
SELECT
  platform,
  SUM(amount_usd) AS total_inflow
FROM
  amount_in_usd
WHERE
  direction = 'inbound'
  AND receipt_succeeded = TRUE
GROUP BY
  platform
ORDER BY
  total_inflow DESC
