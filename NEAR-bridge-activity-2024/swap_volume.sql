--idea from hess swap analysis
with prices as (
  SELECT
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  FROM
    near.defi.ez_dex_swaps
  WHERE
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    and token_in_contract = 'gear.enleap.near'
  GROUP BY
    1,
    2
),
near_price as (
  SELECT
    hour :: date as date,
    'Near' as symbol,
    min(price) as near_price
  from
    near.price.ez_prices_hourly
  GROUP BY
    1,
    2
),
gear as (
  select
    a.date,
    'gear.enleap.near' as tok_address,
    price_ratio * near_price as price
  FROM
    prices a
    JOIN near_price b on a.date = b.date
),
uwon_price as (
  SELECT
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  FROM
    near.defi.ez_dex_swaps
  WHERE
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    and token_in_contract = '438e48ed4ce6beecf503d43b9dbd3c30d516e7fd.factory.bridge.near'
  GROUP BY
    1,
    2
),
uwon as (
  SELECT
    a.date,
    '438e48ed4ce6beecf503d43b9dbd3c30d516e7fd.factory.bridge.near' as tok_address,
    price_ratio * near_price as price
  FROM
    uwon_price a
    JOIN near_price b on a.date = b.date
),
usm_price as (
  SELECT
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  FROM
    near.defi.ez_dex_swaps
  where
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    AND token_in_contract = 'usm.tkn.near'
  GROUP by
    1,
    2
),
usm as (
  SELECT
    a.date,
    'usm.tkn.near' as tok_address,
    price_ratio * near_price as price
  from
    usm_price a
    join near_price b on a.date = b.date
),
dd_price as (
  SELECT
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  FROM
    near.defi.ez_dex_swaps
  WHERE
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    AND token_in_contract = 'dd.tg'
  group by
    1,
    2
),
dd as (
  SELECT
    a.date,
    'dd.tg' as tok_address,
    price_ratio * near_price as price
  FROM
    dd_price a
    join near_price b on a.date = b.date
),
lonk_price as (
  select
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  from
    near.defi.ez_dex_swaps
  WHERE
    symbol_out in ('wNEAR')
    AND amount_out > 0
    and amount_in > 0
    and token_in_contract = 'token.lonkingnearbackto2024.near'
  GROUP BY
    1,
    2
),
lonk as (
  SELECT
    a.date,
    'token.lonkingnearbackto2024.near' as tok_address,
    price_ratio * near_price as price
  from
    lonk_price a
    join near_price b on a.date = b.date
),
bendog_price as (
  SELECT
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  from
    near.defi.ez_dex_swaps
  WHERE
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    AND token_in_contract = 'benthedog.near'
  group by
    1,
    2
),
bendog as (
  SELECT
    a.date,
    'benthedog.near' as tok_address,
    price_ratio * near_price as price
  FROM
    bendog_price a
    join near_price b on a.date = b.date
),
shitzu_price as (
  select
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  from
    near.defi.ez_dex_swaps
  where
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    and token_in_contract = 'token.0xshitzu.near'
  GROUP BY
    1,
    2
),
shitzu as (
  select
    a.date,
    'token.0xshitzu.near' as tok_address,
    price_ratio * near_price as price
  from
    bendog_price a
    join near_price b on a.date = b.date
),
touched_price as (
  select
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  FROM
    near.defi.ez_dex_swaps
  WHERE
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    and token_in_contract = 'touched.tkn.near'
  GROUP BY
    1,
    2
),
touched as (
  SELECT
    a.date,
    'touched.tkn.near' as tok_address,
    price_ratio * near_price as price
  from
    touched_price a
    JOIN near_price b on a.date = b.date
),
kat_price as (
  select
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  from
    near.defi.ez_dex_swaps
  where
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    and token_in_contract = 'kat.token0.near'
  GROUP BY
    1,
    2
),
kat as (
  select
    a.date,
    'kat.token0.near' as tok_address,
    price_ratio * near_price as price
  from
    kat_price a
    JOIN near_price b on a.date = b.date
),
nearnvidia_price as (
  select
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  FROM
    near.defi.ez_dex_swaps
  where
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    and token_in_contract = 'nearnvidia.near'
  group by
    1,
    2
),
nearnvidia as (
  select
    a.date,
    'nearnvidia.near' as tok_address,
    price_ratio * near_price as price
  from
    nearnvidia_price a
    join near_price b on a.date = b.date
),
intel_price as (
  select
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  from
    near.defi.ez_dex_swaps
  where
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    and token_in_contract = 'intel.tkn.near'
  group by
    1,
    2
),
intel as (
  SELECT
    a.date,
    'intel.tkn.near' as tok_address,
    price_ratio * near_price as price
  FROM
    intel_price a
    join near_price b on a.date = b.date
),
nkok_price as (
  select
    block_timestamp :: date as date,
    token_in_contract,
    (sum(amount_out) / sum(amount_in)) as price_ratio
  from
    near.defi.ez_dex_swaps
  where
    symbol_out in ('wNEAR')
    and amount_out > 0
    and amount_in > 0
    and token_in_contract = 'nkok.tkn.near'
  GROUP BY
    1,
    2
) --nkok as (
select
  a.date,
  'nkok.tkn.near' as tok_address,
  price_ratio * price as price
from
  nkok_price a
  join near_price b on a.date = b.date --),
