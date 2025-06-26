with swap_data as (
  SELECT
    date_trunc('day', block_timestamp) as date,
    affiliate_address,
    sum(to_amount_usd / rune_usd) AS swap_volume_rune,
    coalesce(affiliate_fee_basis_points, 0) as affiliate_fee_bp
  FROM
    thorchain.defi.fact_swaps --where date >= '2024-09-01'
  GROUP BY
    1,
    2,
    4
),
swap_fees AS(
  SELECT
    date,
    affiliate_address,
    sum(swap_volume_rune) AS swap_volume_rune,
    affiliate_fee_bp,
    sum(swap_volume_rune * affiliate_fee_bp / 10000) AS affiliate_fee_collected
  FROM
    swap_data
  GROUP BY
    1,
    2,
    4
),
affiliattes_mapped AS(
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
    coalesce(sum(swap_volume_rune), 0) as swap_volume_rune,
    affiliate_fee_bp,
    coalesce(
      sum(swap_volume_rune * affiliate_fee_bp / 10000),
      0
    ) AS affiliate_fee_collected
  FROM
    swap_fees
  group BY
    1,
    2,
    4
  ORDER BY
    3 DESC
),
rune_prices AS(
  SELECT
    date_trunc('day', block_timestamp) as price_date,
    coalesce(avg(rune_usd), 0) as rune_usd_price
  FROM
    thorchain.price.fact_prices
  WHERE
    price_date >= '2024-09-01'
  GROUP by
    1
),
calculated_fees AS(
  SELECT
    am.date,
    am.affiliates,
    am.swap_volume_rune,
    am.affiliate_fee_bp,
    am.affiliate_fee_collected,
    coalesce(
      am.affiliate_fee_collected * rp.rune_usd_price,
      0
    ) AS affiliate_fee_usd
  FROM
    affiliattes_mapped am
    JOIN rune_prices rp on am.date = rp.price_date
),
final_result as (
  SELECT
    date,
    affiliates,
    affiliate_fee_collected as affiliate_fee,
    affiliate_fee_usd,
    sum(affiliate_fee_collected) over () as total_affiliate_fee_rune,
    sum(affiliate_fee_usd) over () as total_affiliate_fee_usd,
    sum(affiliate_fee_collected) over (
      partition BY affiliates
      order by
        date
    ) as cumulative_affiliate_fee,
    sum(affiliate_fee_usd) over (
      partition by affiliates
      order by
        date
    ) as cumulative_affiliate_fee_usd
  FROM
    calculated_fees
)
SELECT
  *
FROM
  final_result
ORDER by
  cumulative_affiliate_fee_usd DESC
