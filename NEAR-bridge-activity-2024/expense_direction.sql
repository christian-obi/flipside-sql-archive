--analysis to see where people spend money
with bridged_in_data as (
  SELECT
    tx_date,
    symbols AS bridged_symbol,
    amount as bridged_amount,
    amount_usd as bridged_amount_usd,
    receipt_succeeded,
    destination_address,
    tx_hash as bridged_tx_hash,
    token_address as bridged_token_address
  FROM
    $ query('f5223835-ccf0-408d-94a5-a14577ec6915')
  WHERE
    receipt_succeeded = TRUE
),
swap_data as (
  SELECT
    block_timestamp,
    tx_hash as swap_tx_hash,
    trader,
    platform,
    pool_id,
    amount_in AS swap_amount_in,
    amount_in_usd as swap_amount_in_usd,
    symbol_in as swap_symbol_in,
    amount_out as swap_amount_out,
    amount_out_usd as swap_amount_out_usd,
    symbol_out as swap_symbol_out
  from
    near.defi.ez_dex_swaps
)
SELECT
  b.tx_date as bridged_tx_date,
  b.bridged_symbol,
  b.bridged_amount,
  b.bridged_amount_usd,
  b.destination_address as bridged_to_address,
  date_trunc('day', s.block_timestamp) as swap_tx_date,
  s.swap_symbol_in,
  s.swap_amount_in,
  s.swap_amount_in_usd,
  s.swap_symbol_out,
  s.swap_amount_out,
  s.swap_amount_out_usd,
  s.platform
FROM
  bridged_in_data b
  LEFT JOIN swap_data s on b.destination_address = s.trader
  and b.tx_date <= s.block_timestamp
order BY
  bridged_tx_date,
  swap_tx_date DESC
