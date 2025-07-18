with og_mint_table as (
  SELECT
    *
  FROM
    core.nft.ez_nft_transfers
  where
    date_trunc ('day', block_timestamp) = '2024-12-13'
    and contract_address = '0xe48696582061011beadcdb1eb132ff2261ced5cf'
    and is_mint = 'TRUE'
)
SELECT
  to_address as address,
  sum(quantity) as number_of_NFT,
  count(address) over () as total_og_address,
  sum(number_of_NFT) over () as total_og_mint
FROM
  og_mint_table
GROUP by
  1
ORDER BY
  2 DESC
