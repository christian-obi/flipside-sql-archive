with pell as (
  SELECT
    to_timestamp(f.value:date) as date,
    f.value:totalLiquidityUSD as pell_tvl
  FROM
    lateral flatten(
      input = > defillama.get('/protocol/pell-network', { }) :data:chainTvls:CORE:tvl
    ) f
),
colend as(
  SELECT
    to_timestamp(f.value:date) as date,
    f.value:totalLiquidityUSD as colend_tvl
  from
    lateral flatten(
      input = > defillama.get('/protocol/colend-protocol', { }) :data:chainTvls:CORE:tvl
    ) f
),
desyn as (
  SELECT
    to_timestamp(f.value:date) as date,
    f.value:totalLiquidityUSD as desyn_tvl
  from
    lateral flatten(
      input = > defillama.get('/protocol/desyn-liquid-strategy', { }) :data:chainTvls:CORE:tvl
    ) f
),
avalon as(
  select
    to_timestamp(f.value:date) as date,
    f.value:totalLiquidityUSD as avalon_tvl
  FROM
    lateral flatten(
      input = > defillama.get('/protocol/desyn-liquid-strategy', { }) :data:chainTvls:CORE:tvl
    ) f
),
corex as(
  SELECT
    to_timestamp(f.value:date) as date,
    f.value:totalLiquidityUSD as corex_tvl
  FROM
    lateral flatten(
      input = > defillama.get('/protocol/corex-network', { }) :data:chainTvls:CORE:tvl
    ) f
)
SELECT
  p.date,
  p.pell_tvl,
  c.colend_tvl,
  d.desyn_tvl,
  a.avalon_tvl,
  x.corex_tvl
from
  pell p
  join colend c on p.date = c.date
  join desyn d on p.date = d.date
  join avalon a on p.date = a.date
  join corex x on p.date = x.date
where
  p.date >= '2024-06-01' --'{{start_date}}'
order by
  1
