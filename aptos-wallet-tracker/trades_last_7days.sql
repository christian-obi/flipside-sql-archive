SELECT
  count(tx_hash) as trade_count
from
  aptos.nft.ez_nft_sales
WHERE
  (
    buyer_address = '{{walletaddress}}'
    or seller_address = '{{walletaddress}}'
  )
  AND block_timestamp >= current_date -7
