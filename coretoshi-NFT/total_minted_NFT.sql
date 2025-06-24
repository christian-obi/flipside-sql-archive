SELECT
  count(tx_hash) as "Number of coretoshi minted"
FROM
  core.nft.ez_nft_transfers
where
  contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
  and is_mint = TRUE
