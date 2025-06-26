--owners of rosentica nft from highest to lowest 
with base as (
  SELECT
    buyer_address,
    tokenid
  FROM
    ethereum.nft.ez_nft_sales
  WHERE
    nft_address = lower('0x86486fE85545b57D06330acf1f3D63bb7b790cB4') --and platform_address = '0x00000000000000adc04c56bf30ac9d3c0aaf14dc'
    --and platform_name ='opensea'
    qualify row_number() over (
      partition by tokenid
      order by
        block_timestamp desc
    ) = 1
)
SELECT
  buyer_address as NFT_owners,
  count(1) as nft_count
from
  base
group by
  all
ORDER by
  nft_count desc
limit
  10
