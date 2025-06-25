with near_tx as (
  SELECT
    date_trunc('day', block_timestamp) as tx_date,
    count(DISTINCT tx_hash) as near_transaction,
  FROM
    near.defi.ez_bridge_activity
  WHERE
    receipt_succeeded = TRUE
    and tx_date >= current_date -60
  group BY
    tx_date
  order by
    1 DESC
),
base_tx as (
  SELECT
    date_trunc('day', block_timestamp) as date,
    count(DISTINCT tx_hash) as base_transaction
  FROM
    base.defi.ez_bridge_activity
  WHERE
    date >= current_date -60
    and event_name in (
      'TransferSent',
      'FundsDeposited',
      'V3FundsDeposited',
      'Transfer',
      'Swap',
      'Send',
      'TokenRedeemAndSwap'
    )
  GROUP BY
    1
  ORDER by
    1 DESC
)
SELECT
  nt.tx_date,
  nt.near_transaction,
  bt.base_transaction
FROM
  near_tx nt
  join base_tx bt on nt.tx_date = bt.date
ORDER BY
  1 DESC
