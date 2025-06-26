with affiliate_fee as(
  SELECT
    date_trunc('day', block_timestamp) as date,
    affiliate_address,
    sum(
      (liq_fee_rune * 100) * (affiliate_fee_basis_points / 10000)
    ) as affiliate_fee_rune,
    sum(
      (liq_fee_rune_usd * 100) * (affiliate_fee_basis_points / 10000)
    ) as affiliate_fee_usd
  FROM
    thorchain.defi.fact_swaps
  where
    date >= '2024-09-01'
  GROUP BY
    1,
    2
),
affiliates_mapped AS(
  SELECT
    date,
    CASE
      when affiliate_address IN(
        't',
        'T',
        'thor160yye65pf9rzwrgqmtgav69n6zlsyfpgm9a7xk'
      ) then 'ThorSwap'
      when affiliate_address in(
        'wr',
        'thor1a427q3v96psuj4fnughdw8glt5r7j38lj7rkp8'
      ) then 'ThorWallet'
      when affiliate_address IN('ti', 'te', 'tr', 'td') then 'TrustWallet'
      when affiliate_address = 'tl' then 'TS Ledger'
      when affiliate_address = 'cb' then 'Team CoinBot'
      when affiliate_address = 'dx' then 'Asgardex'
      when affiliate_address = 'ss' then 'Shapeshift'
      when affiliate_address = 'xdf' then 'Xdefi'
      when affiliate_address = 'rg' then 'Rango'
      when affiliate_address = 'ej' then 'Edge Wallet'
      when affiliate_address = 'ds' then 'DefiSpot'
      when affiliate_address = 'lifi' then 'Lifi'
      when affiliate_address = 'oky' then 'OneKey Wallet'
      when affiliate_address = 'sy' then 'Symbiosis'
      when affiliate_address = 'vi' then 'Vultisig'
      when affiliate_address = 'cakewallet' then 'CakeWallet'
      when affiliate_address = 'lends' then 'Lends'
      when affiliate_address is null then 'No Affiliate'
    end as affiliates,
    coalesce(sum(affiliate_fee_rune), 0) as affiliate_fee_rune,
    coalesce(sum(affiliate_fee_usd), 0) AS affiliate_fee_usd
  FROM
    affiliate_fee
  group BY
    1,
    2
  ORDER BY
    1 DESC
),
rune_prices AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS price_date,
    COALESCE(AVG(RUNE_USD), 0) AS rune_usd_price
  FROM
    thorchain.price.fact_prices
  WHERE
    block_timestamp :: date >= '2024-09-01' --AND block_timestamp::date <= '2024-10-31'
  GROUP BY
    price_date
),
calculated_fees AS (
  SELECT
    am.date,
    am.Affiliates,
    COALESCE(am.affiliate_fee_rune * rp.rune_usd_price, 0) AS affiliate_fee_rune,
    COALESCE(am.affiliate_fee_usd * rp.rune_usd_price, 0) AS affiliate_fee_usd
  FROM
    affiliates_mapped am
    JOIN rune_prices rp ON am.date = rp.price_date
)
SELECT
  date,
  affiliates,
  sum(affiliate_fee_rune) as daily_fee_rune,
  sum(affiliate_fee_usd) as daily_fee_usd,
FROM
  calculated_fees
where
  affiliates IN (
    'TrustWallet',
    'ThorSwap',
    'ThorWallet',
    'Lends',
    'Shapeshift',
    'Asgardex',
    'Edge Wallet',
    'Xdefi',
    'Vultisig'
  )
group by
  1,
  2
ORDER by
  1
