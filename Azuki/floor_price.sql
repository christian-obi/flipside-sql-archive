--azuki => '0xed5af388653567af2f388e6224dc7c4b3241c544'  
--azuki elementals => '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e'  
--azuki red beans => '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949'  
--azuki elemental beanz => '0x3af2a97414d1101e2107a70e7f33955da1346305'


with azuki_floor as (
  select
    platform_address,
    platform_name,
    nft_address as address,
    min(price_usd) as floor_price_usd,
    min(price) as floor_price_eth,
  FROM
    ethereum.nft.ez_nft_sales
  where
    platform_address in (
      '0x0000000000000068f116a894984e2db1123eb395',
      '0x88ec0445f3bbbe2b1f8ab9dea9cacecad4992334',
      '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3',
      '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642',
      '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5'
    )
    and nft_address = '0xed5af388653567af2f388e6224dc7c4b3241c544'
    and block_timestamp >= '2024-09-01'
  GROUP by
    1,
    2,
    3
),
azuki_elementals_floor as (
  select
    platform_address,
    platform_name,
    nft_address as address,
    min(price_usd) as floor_price_usd,
    min(price) as floor_price_eth,
  FROM
    ethereum.nft.ez_nft_sales
  where
    platform_address in (
      '0x0000000000000068f116a894984e2db1123eb395',
      '0x88ec0445f3bbbe2b1f8ab9dea9cacecad4992334',
      '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3',
      '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642',
      '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5'
    )
    and nft_address = '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e'
    and block_timestamp >= '2024-09-01'
  GROUP by
    1,
    2,
    3
),
azuki_beanz as(
  select
    platform_address,
    platform_name,
    nft_address as address,
    min(price_usd) as floor_price_usd,
    min(price) as floor_price_eth,
  FROM
    ethereum.nft.ez_nft_sales
  where
    platform_address in (
      '0x0000000000000068f116a894984e2db1123eb395',
      '0x88ec0445f3bbbe2b1f8ab9dea9cacecad4992334',
      '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3',
      '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642',
      '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5'
    )
    and nft_address = '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949'
    and block_timestamp >= '2024-09-01'
  GROUP by
    1,
    2,
    3
),
azuki_elemental_beanz AS(
  select
    platform_address,
    platform_name,
    nft_address as address,
    min(price_usd) as floor_price_usd,
    min(price) as floor_price_eth,
  FROM
    ethereum.nft.ez_nft_sales
  where
    platform_address in (
      '0x0000000000000068f116a894984e2db1123eb395',
      '0x88ec0445f3bbbe2b1f8ab9dea9cacecad4992334',
      '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3',
      '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642',
      '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5'
    )
    and nft_address = '0x3af2a97414d1101e2107a70e7f33955da1346305'
    and block_timestamp >= '2024-09-01'
  GROUP by
    1,
    2,
    3
),
floor_price as(
  SELECT
    *
  FROM
    azuki_floor
  UNION
  SELECT
    *
  FROM
    azuki_elementals_floor
  UNION
  SELECT
    *
  FROM
    azuki_beanz
  UNION
  SELECT
    *
  FROM
    azuki_elemental_beanz
)
SELECT
  platform_address,
  address,
  floor_price_usd,
  floor_price_eth,
  CASE
    when platform_address = '0x0000000000000068f116a894984e2db1123eb395' then 'opensea'
    when platform_address = '0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5' then 'blur'
    when platform_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3' then 'x2y2'
    when platform_address = '0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642' then 'magic eden'
    when platform_address = '0x88ec0445f3bbbe2b1f8ab9dea9cacecad4992334' then 'sudoswap'
  end as platform_name,
  CASE
    when address = '0xed5af388653567af2f388e6224dc7c4b3241c544' then 'Azuki'
    when address = '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e' then 'Azuki elementals'
    when address = '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949' then 'Azuki beanz'
    when address = '0x3af2a97414d1101e2107a70e7f33955da1346305' then 'Azuki elemental beanz'
  end as NFT_name
FROM
  floor_price
WHERE
  floor_price_usd != 0
  AND floor_price_eth != 0.81
order by
  NFT_name 




-- opensea => 0x0000000000000068f116a894984e2db1123eb395, 0x00000000000000adc04c56bf30ac9d3c0aaf14dc
  -- element
  -- sudoswap => 0x88ec0445f3bbbe2b1f8ab9dea9cacecad4992334
  -- x2y2 => 0x74312363e45dcaba76c59ec49a7aa8a65a67eed3
  -- magic eden => 0x5ebc127fae83ed5bdd91fc6a5f5767e259df5642, 0xb233e3602bb06aa2c2db0982bbaf33c2b15184c9
  -- blur => 0xb2ecfe4e4d61f8790bbb9de2d1259b9e2410cea5, 0x29469395eaf6f95920e59f858042f0e28d98a20b, 0x000000000000ad05ccc4f10045630fb830b95127
