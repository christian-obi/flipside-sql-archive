-- SELECT
-- DISTINCT token_address,
-- symbol,
-- decimals,
-- blockchain
-- from near.price.ez_prices_hourly
-- where symbol IN ('NEAR', 'WETH','WNEAR', 'WBTC','USDC.e','USDT.e', 'DAI','AURORA','STNEAR', 'USDC','USDT', 'SOL', 'LINEAR')
SELECT
*
FROM near.defi.ez_bridge_activity
where platform ilike 'wormhole'
--where symbol IN ('WNEAR')
-- case when symbol = 'AURORA' then (amount_adj/ pow(10,18))
--      when symbol = 'WBTC' then (amount_adj/ pow(10,8))
--     when symbol = 'USDC' then (amount_adj/ pow(10,6))
--     when symbol = 'USDC.e' then (amount_adj/ pow(10,6)
--     when symbol = 'USDT.e' then (amount_adj/ pow(10,6)
-- when symbol = 'WETH' then (amount_adj/ pow(10,18))
-- when symbol = 'DAI' then (amount_adj/ pow(10,18))
-- when symbol = 'STNEAR' then (amount_adj/ pow(10,24))
--when symbol = 'WETH' then (amount_adj/ pow(10,18))
-- when symbol = 'LINEAR' then (amount_adj/ pow(10,24))
--      when symbol = 'SOL' then (amount_adj/ pow(10,8))
--    when symbol = 'USDT' then (amount_adj/ pow(10,6)
--      when symbol = 'NEAR' then (amount_adj/ pow(10,8))





--group by 1,2
limit 15
