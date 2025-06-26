-- top address bridge out based on volume
with raw as (
  SELECT
    sender as wallet_address,
    round(sum(amount_in_usd), 1) as total_bridged_volume,
  from
    aptos.defi.ez_bridge_activity
  where
    direction = 'outbound'
    and block_timestamp >= '{{start_date}}'
    and amount_in_usd > 1
  group BY
    1
  ORDER by
    2 DESC
  limit
    10
)
SELECT
  wallet_address,
  '💲 ' || total_bridged_volume as total_outbound_volume_usd
from
  raw
