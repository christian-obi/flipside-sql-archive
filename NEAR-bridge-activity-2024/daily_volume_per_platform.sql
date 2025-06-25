-- forked from hess / Daily Volume per Platform @ https://flipsidecrypto.xyz/hess/q/OxCavzNvcPJd/daily-volume-per-platform
WITH prices as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and date(block_timestamp) >= '2023-12-10'
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'gear.enleap.near'
  group by
    1,
    2
),
near_price as (
  select
    hour :: date as date,
    'Near' as symbol,
    min(price) as near_price
  from
    near.price.ez_prices_hourly
  where
    symbol = 'NEAR'
    and hour :: date >= '2023-12-10'
  group by
    1,
    2
),
gear_price as (
  select
    a.date,
    price * near_price as gear_price
  from
    prices a
    join near_price b on a.date = b.date
),
final_gear as (
  select
    a.date,
    b.gear_price,
    a.near_price
  from
    near_price a
    left join gear_price b on a.date = b.date
  order by
    a.date
),
price as (
  select
    date,
    gear_price as price,
    'gear.enleap.near' as token_address
  from
    final_gear
  UNION
  select
    hour :: date as date,
    avg(price) as price,
    token_address
  from
    near.price.ez_prices_hourly
  where
    token_address not in (
      'token.burrow.near',
      'token.lonkingnearbackto2024.near',
      'gear.enleap.near',
      'token.0xshitzu.near',
      'blackdragon.tkn.near'
    )
  group by
    1,
    3
  UNION
  select
    hour :: date as date,
    avg(price) as price,
    '23.contract.portalbridge.near' as token_address
  from
    near.price.ez_prices_hourly
  where
    symbol = 'AURORA'
  group by
    1,
    3
  UNION
  select
    hour :: date as date,
    avg(price) as price,
    'aurora' as token_address
  from
    ethereum.price.ez_prices_hourly
  where
    symbol = 'WETH'
  group by
    1,
    3
),
swaps_v1 as (
  select
    block_timestamp,
    tx_hash,
    platform,
    pool_id,
    trader,
    amount_out,
    amount_out_usd,
    token_out_contract,
    symbol_out,
    amount_in,
    amount_in_usd,
    token_in_contract,
    symbol_in,
    case
      when tx_hash in (
        select
          DISTINCT tx_hash
        from
          near.core.fact_receipts
        where
          array_to_string(logs, ' ') ilike '%owner.herewallet.near%'
      ) then 'Here Wallet'
      else platform
    end as platforms,
    amount_in * b.price as amt_i_usd,
    amount_out * c.price as amt_o_usd,
    case
      when amt_i_usd is not null then amt_i_usd
      else amount_in_usd
    end as amount_i_usd,
    case
      when amt_o_usd is not null then amt_o_usd
      else amount_out_usd
    end as amount_o_usd
  from
    near.defi.ez_dex_swaps a
    left outer join price b on a.block_timestamp :: date = b.date
    and a.token_in_contract = b.token_address
    left outer join price c on a.block_timestamp :: date = c.date
    and a.token_out_contract = c.token_address
  where
    block_timestamp :: date >= '2024-01-01'
),
swaps as (
  select
    block_timestamp,
    tx_hash,
    platform,
    pool_id,
    trader,
    amount_out,
    amount_o_usd,
    token_out_contract,
    symbol_out,
    amount_in,
    amount_i_usd,
    token_in_contract,
    symbol_in,
    platforms,
    case
      when (
        symbol_in ilike '%usdt%'
        or symbol_in ilike '%usdc%'
        or symbol_in ilike '%dai%'
      ) then amount_in
    end as amt_in_us,
    case
      when (
        symbol_out ilike '%usdt%'
        or symbol_out ilike '%usdc%'
        or symbol_out ilike '%dai%'
      ) then amount_out
    end as amt_out_us,
    case
      when amt_in_us is not null then amt_in_us
      else amount_i_usd
    end as amt_in_usd,
    case
      when amt_out_us is not null then amt_out_us
      else amount_o_usd
    end as amt_out_usd,
    case
      when amt_in_usd is null then amt_out_usd
      else amt_in_usd
    end as volume,
    case
      when amt_in_usd > amt_out_usd then amt_out_usd
      else amt_in_usd
    end as volume_ii,
    case
      when volume_ii is null then volume
      else volume_ii
    end as volume_usd
  from
    swaps_v1
  where
    block_timestamp :: date >= '2024-01-01'
    and symbol_in != 'PEM'
    and symbol_out != 'PEM'
),
min as (
  select
    min(block_timestamp :: date) as date,
    trader
  from
    near.defi.ez_dex_swaps
  group by
    2
),
new as (
  select
    DISTINCT trader
  from
    min
  where
    date >= '2024-01-01'
)
select
  block_timestamp :: Date as date,
  case
    when platforms ilike '%ref%' then 'Ref Finance'
    when platforms = 'v1.jumbo_exchange.near' then 'Jumbo'
    else platforms
  end as platform,
  sum(volume_usd) as usd_volume,
  count(DISTINCT trader) as traders
from
  swaps
group by
  1,
  2
