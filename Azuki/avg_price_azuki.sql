-- avg sale price azuki

--azuki => '0xed5af388653567af2f388e6224dc7c4b3241c544'  
--azuki elementals => '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e'  
--azuki red beans => '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949'  
--azuki elemental beanz => '0x3af2a97414d1101e2107a70e7f33955da1346305'
with price_stats as (
  SELECT
    min(price_usd) as min_price,
    percentile_cont(0.5) within GROUP (
      ORDER by
        price_usd
    ) as mid_price,
    max(price_usd) as max_price
  FROM
    ethereum.nft.ez_nft_sales
  where
    nft_address = '0xed5af388653567af2f388e6224dc7c4b3241c544'
    AND block_timestamp >= '2024-08-01'
),
binned_sales as (
  SELECT
   
    price_usd,
    date_trunc('day', block_timestamp) as date
  FROM
    ethereum.nft.ez_nft_sales
  where
    nft_address = '0xed5af388653567af2f388e6224dc7c4b3241c544'
    and block_timestamp >= '2024-08-01'
) 
select
  date,
  avg(price_usd) as avg_sale_price,
  CASE
    when avg_sale_price <= 11455 then 'low sale price range'
    when avg_sale_price > 11455
    and avg_sale_price <= 12261.44 then 'mid sale price range'
    when avg_sale_price > 12261.44
    and avg_sale_price <= 49291 then 'high sale price range'
    else 'above max price'
  end as price_bin,
from
  binned_sales
group by
  1
ORDER by
  1
