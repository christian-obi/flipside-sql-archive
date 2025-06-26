with nfts_bought as (
  select
    tokenid,
    count(*) as bought_count
  FROM
    aptos.nft.ez_nft_sales
  WHERE
    buyer_address = '{{walletaddress}}'
  GROUP BY
    tokenid
),
nfts_sold as (
  SELECT
    tokenid,
    count(*) as sold_count
  from
    aptos.nft.ez_nft_sales
  WHERE
    seller_address = '{{walletaddress}}'
  GROUP BY
    tokenid
),
ch as (
  SELECT
    b.tokenid,
    b.bought_count - coalesce(s.sold_count, 0) as currently_held
  FROM
    nfts_bought b
    LEFT join nfts_sold s on b.tokenid = s.tokenid
  WHERE
    (b.bought_count - coalesce(s.sold_count, 0)) > 0
  ORDER BY
    2
)
SELECT
  sum(currently_held) as holdings
FROM
  ch
