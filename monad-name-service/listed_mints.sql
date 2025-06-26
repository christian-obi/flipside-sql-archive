-- minter that listed on magic eden
with curated_table as (
  select
    date_trunc('day', block_timestamp) as listing_date,
    count(tx_hash) as total_listing,
    --count(tx_hash) over () as cumulative_listing,
  from
    monad.testnet.fact_traces
  where
    0 = 0
    and to_address = '0x3019bf1dfb84e5b46ca9d0eec37de08a59a41308'
    and origin_function_signature = '0xa22cb465'
    and block_timestamp >= '2025-02-19'
  group by
    1 --order by listing_date
)
select
  *,
  sum(total_listing) over (
    order by
      listing_date
  ) as cumulative_listing
from
  curated_table
order by
  listing_date desc
