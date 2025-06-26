with minted_tokens as (
  select
    block_timestamp,
    tx_hash,
    origin_from_address as minter
  from
    monad.testnet.fact_event_logs
  where
    contract_address = '0x3019bf1dfb84e5b46ca9d0eec37de08a59a41308'
    and origin_to_address = '0x758d80767a751fc1634f579d76e1ccaab3485c9c'
    and origin_function_signature = '0xadba7a51'
    and topic_0 = '0xcee7d9f7ea527be6fd2ad58f0621348b614e640f77b4e69c5546e2a68ac4db20'
    and tx_succeeded = TRUE
)
select
  minter,
  count(distinct tx_hash) as total_domains
from
  minted_tokens
group by
  1
order by
  total_domains desc
limit
  100
