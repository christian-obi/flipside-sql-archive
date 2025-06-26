SELECT
  CASE
    when pool_name = 'ETH.USDT-0XDAC17F958D2EE523A2206206994597C13D831EC7' then 'ETH.USDT'
    when pool_name = 'ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48' then 'ETH.USDC'
    when pool_name = 'ETH.THOR-0XA5F2211B9B8170F694421F2046281775E8468044' then 'ETH.THOR'
    when pool_name = 'ETH.WBTC-0X2260FAC5E5542A773AA44FBCFEDF7C193BC2C599' then 'ETH.WBTC'
    else pool_name
  end as pool,
  day,
  asset_liquidity * asset_price_usd as pool_size_usd,
  rune_liquidity * rune_price_usd as rune_size_usd,
  pool_size_usd + rune_size_usd as total_volume
FROM
  thorchain.defi.fact_daily_pool_stats
WHERE
  pool_name != 'THOR.TOR'
  and day = current_date
ORDER by
  total_volume DESC
limit
  10
