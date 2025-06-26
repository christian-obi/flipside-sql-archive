-- total sale volume

--azuki => '0xed5af388653567af2f388e6224dc7c4b3241c544'  
--azuki elementals => '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e'  
--azuki red beans => '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949'  
--azuki elemental beanz => '0x3af2a97414d1101e2107a70e7f33955da1346305'
SELECT
  sum(price_usd) as total_sales_volume_usd,
  nft_address,
  CASE
    when nft_address = '0xed5af388653567af2f388e6224dc7c4b3241c544' then 'Azuki'
    when nft_address = '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e' then 'Azuki Elementals'
    when nft_address = '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949' then 'Azuki Beanz'
    when nft_address = '0x3af2a97414d1101e2107a70e7f33955da1346305' then 'Azuki Elemental Beanz'
  end as nft_name --project_name
FROM
  ethereum.nft.ez_nft_sales
where
  nft_address in (
    '0xed5af388653567af2f388e6224dc7c4b3241c544',
    '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e',
    '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949',
    '0x3af2a97414d1101e2107a70e7f33955da1346305'
  )
GROUP by
  2,
  3
