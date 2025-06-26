-- total spent by addresses
SELECT
  sum(total_price_usd) as total_spent
FROM
  aptos.nft.ez_nft_sales
WHERE
  buyer_address = '{{walletaddress}}'
