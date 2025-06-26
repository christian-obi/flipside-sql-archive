SELECT
  count(DISTINCT tx_hash) as trans
FROM
  aptos.nft.ez_nft_sales
