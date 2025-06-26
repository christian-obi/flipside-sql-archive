SELECT
  block_timestamp,
  CASE
    when buyer_address = '{{walletaddress}}' then tokenid
    else NULL
  end as nft_bought,
  CASE
    when seller_address = '{{walletaddress}}' then tokenid
    else NULL
  end as nft_sold,
  platform_name,
  total_price,
  total_price_usd,
  nft_address
FROM
  aptos.nft.ez_nft_sales
WHERE
  (
    buyer_address = '{{walletaddress}}'
    or seller_address = '{{walletaddress}}'
  )
  and block_timestamp >= current_date -7
ORDER BY
  block_timestamp DESC
