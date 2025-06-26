--azuki => '0xed5af388653567af2f388e6224dc7c4b3241c544'  
--azuki elementals => '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e'  
--azuki red beans => '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949'  
--azuki elemental beanz => '0x3af2a97414d1101e2107a70e7f33955da1346305'
SELECT
  count(*) as total_no_sales,
  date_trunc('day', block_timestamp) as day,
  --block_timestamp::date as day,
  sum(price_usd) as sale_per_day,
  sum(price) as sale_per_day_eth
FROM
  ethereum.nft.ez_nft_sales
where
  nft_address = '0xed5af388653567af2f388e6224dc7c4b3241c544'
  and day >= '2024-08-01'
group by
  2
order by
  2
