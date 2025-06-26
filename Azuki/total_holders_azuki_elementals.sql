--azuki => '0xed5af388653567af2f388e6224dc7c4b3241c544'  
--azuki elementals => '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e'  
--azuki red beans => '0x306b1ea3ecdf94ab739f1910bbda052ed4a9f949'  
--azuki elemental beanz => '0x3af2a97414d1101e2107a70e7f33955da1346305'
with latest_transfers as(
  SELECT
    tokenid,
    nft_to_address as holder,
    row_number() over (
      partition by tokenid
      order by
        block_timestamp desc
    ) as rank
  FROM
    ethereum.nft.ez_nft_transfers
  where
    nft_address = '0xb6a37b5d14d502c3ab0ae6f3a0e058bc9517786e'
)
SELECT
  count(DISTINCT holder) as unique_holders
FROM
  latest_transfers
where
  rank = 1
