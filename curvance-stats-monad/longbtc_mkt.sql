with base as (
  select
    block_timestamp as tx_date,
    tx_hash,
    from_address as user,
    value_precise,
    tx_fee_precise as tx_fee_mon,
    cumulative_gas_used,
  from
    monad.testnet.fact_transactions
  where
    0 = 0
    and to_address = '0xcda16e9c25f429f4b01a87ff302ee7943f2d5015'
    and origin_function_signature = '0xe8bbf5d7'
    and tx_succeeded = 'TRUE'
    and block_timestamp >= '2025-02-18'
)
select
  date_trunc('day', tx_date) as tx_date,
  count(distinct tx_hash) as total_transactions,
  count(distinct user) as total_user,
  sum(tx_fee_mon) total_fee_generated,
  sum(cumulative_gas_used) as total_gas_used,
  sum(total_transactions) over () as cumulative_tx,
  --count(distinct user) over () as total_wallets,
  sum(total_fee_generated) over () as cumulative_fee,
  sum(total_gas_used) over () as cumulative_gas
from
  base
group by
  1
order by
  1 desc
