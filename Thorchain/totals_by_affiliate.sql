with structure as (
  SELECT
    tx_id,
    block_timestamp :: date as day,
    to_pool_address,
    nvl(from_amount_usd, to_amount_usd) as amount_usd,
    from_address as user,
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
  FROM
    thorchain.defi.fact_swaps
)
SELECT
  day,
  affiliates,
  count(DISTINCT user) as users,
FROM
  structure
WHERE
  day >= current_date - 31
GROUP by
  1,
  2
ORDER by
  1
