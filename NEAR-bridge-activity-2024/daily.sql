WITH prices as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
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
    and hour :: date >= '2024-01-01'
  group by
    1,
    2
),
gear as (
  select
    a.date,
    'gear.enleap.near' as tok_address,
    price * near_price as price
  from
    prices a
    join near_price b on a.date = b.date
),
uwon_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = '438e48ed4ce6beecf503d43b9dbd3c30d516e7fd.factory.bridge.near'
  group by
    1,
    2
),
uwon as (
  select
    a.date,
    '438e48ed4ce6beecf503d43b9dbd3c30d516e7fd.factory.bridge.near' as tok_address,
    price * price as price
  from
    uwon_price a
    join near_price b on a.date = b.date
),
usm_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'usm.tkn.near'
  group by
    1,
    2
),
usm as (
  select
    a.date,
    'usm.tkn.near' as tok_address,
    price * price as price
  from
    usm_price a
    join near_price b on a.date = b.date
),
dd_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'dd.tg'
  group by
    1,
    2
),
dd as (
  select
    a.date,
    'dd.tg' as tok_address,
    price * price as price
  from
    dd_price a
    join near_price b on a.date = b.date
),
lonk_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'token.lonkingnearbackto2024.near'
  group by
    1,
    2
),
lonk as (
  select
    a.date,
    'token.lonkingnearbackto2024.near' as tok_address,
    price * price as price
  from
    lonk_price a
    join near_price b on a.date = b.date
),
bendog_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'benthedog.near'
  group by
    1,
    2
),
bendog as (
  select
    a.date,
    'benthedog.near' as tok_address,
    price * price as price
  from
    bendog_price a
    join near_price b on a.date = b.date
),
shitzu_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'token.0xshitzu.near'
  group by
    1,
    2
),
shitzu as (
  select
    a.date,
    'token.0xshitzu.near' as tok_address,
    price * price as price
  from
    bendog_price a
    join near_price b on a.date = b.date
),
touched_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'touched.tkn.near'
  group by
    1,
    2
),
touched as (
  select
    a.date,
    'touched.tkn.near' as tok_address,
    price * price as price
  from
    touched_price a
    join near_price b on a.date = b.date
),
kat_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'kat.token0.near'
  group by
    1,
    2
),
kat as (
  select
    a.date,
    'kat.token0.near' as tok_address,
    price * price as price
  from
    kat_price a
    join near_price b on a.date = b.date
),
nearnvidia_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'nearnvidia.near'
  group by
    1,
    2
),
nearnvidia as (
  select
    a.date,
    'nearnvidia.near' as tok_address,
    price * price as price
  from
    nearnvidia_price a
    join near_price b on a.date = b.date
),
intel_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'intel.tkn.near'
  group by
    1,
    2
),
intel as (
  select
    a.date,
    'intel.tkn.near' as tok_address,
    price * price as price
  from
    intel_price a
    join near_price b on a.date = b.date
),
nkok_price as (
  select
    date(block_timestamp) as date,
    TOKEN_IN_CONTRACT,
    (sum(AMOUNT_OUT) / sum(AMOUNT_IN)) as price
  from
    near.defi.ez_dex_swaps
  where
    SYMBOL_OUT in ('wNEAR')
    and AMOUNT_OUT > 0
    and AMOUNT_IN > 0
    and TOKEN_IN_CONTRACT = 'nkok.tkn.near'
  group by
    1,
    2
)
select
  a.date,
  'nkok.tkn.near' as tok_address,
  price * price as price
from
  nkok_price a
  join near_price b on a.date = b.date
