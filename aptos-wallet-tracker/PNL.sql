-- profit/loss of address
with purchases as (
  SELECT
    tokenid,
    min(block_timestamp) as purchase_date,
    min(total_price_usd) as buy_price
  FROM
    aptos.nft.ez_nft_sales
  WHERE
    buyer_address = '{{walletaddress}}'
  GROUP by
    tokenid
),
sales as (
  SELECT
    tokenid,
    min(block_timestamp) as sale_date,
    min(total_price_usd) as sell_price
  from
    aptos.nft.ez_nft_sales
  WHERE
    seller_address = '{{walletaddress}}'
  GROUP BY
    tokenid
),
pl as (
  SELECT
    p.tokenid,
    p.purchase_date,
    s.sale_date,
    p.buy_price,
    s.sell_price,
    (s.sell_price - p.buy_price) as profit_loss
  FROM
    purchases p
    LEFT JOIN sales s on p.tokenid = s.tokenid
  ORDER BY
    p.purchase_date
)
SELECT
  sum(profit_loss) as pl_amount_usd
FROM
  pl
