-- top 10 buyers across markets
with raw as (
  SELECT
    buyer_address,
    round(sum(total_price_usd), 1) as total_spent_usd
  FROM
    aptos.nft.ez_nft_sales
  GROUP by
    1
  order by
    2 DESC
  LIMIT
    10
)
SELECT
  buyer_address,
  '💲 ' || total_spent_usd as total_USD
FROM
  raw
