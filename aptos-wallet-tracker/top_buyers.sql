-- top 10 buyer across market by name
with top_buyer as(
  SELECT
    buyer_address,
    sum(total_price_usd) as total_spent_usd
  FROM
    aptos.nft.ez_nft_sales
  GROUP by
    1
  order by
    2 DESC
  LIMIT
    10
), no_mrkt as(
  SELECT
    buyer_address as buyer_count,
    count(DISTINCT platform_name) as marketplaces_count
  FROM
    aptos.nft.ez_nft_sales
  GROUP BY
    1
  HAVING
    count(DISTINCT platform_name) > 1
  ORDER BY
    marketplaces_count DESC
),
mrkt_name as (
  SELECT
    buyer_address as buyer,
    listagg(DISTINCT platform_name, ',') as marketplaces
  FROM
    aptos.nft.ez_nft_sales
  group by
    1
  having
    count(DISTINCT platform_name) > 1
  ORDER BY
    1
)
SELECT
  tb.buyer_address,
  '💲 ' || round(tb.total_spent_usd, 1) as total_usd,
  nm.marketplaces_count as platforms,
  mn.marketplaces
FROM
  top_buyer tb
  inner JOIN no_mrkt nm on tb.buyer_address = nm.buyer_count
  INNER JOIN mrkt_name mn on nm.buyer_count = mn.buyer
ORDER BY
  tb.total_spent_usd DESC
