with total_volume as (
  SELECT
    date_trunc('week', block_timestamp) as date,
    platform_name,
    sum(total_price_usd) as volume_usd,
    sum(total_price) as volume,
    sum(platform_fee) as fee,
    sum(platform_fee_usd) as fee_usd,
    sum(creator_fee) as creators_fee,
    sum(creator_fee_usd) as creators_fee_usd,
  FROM
    aptos.nft.ez_nft_sales
  group by
    1,
    2
  order by
    1
)
SELECT
  *
from
  total_volume
