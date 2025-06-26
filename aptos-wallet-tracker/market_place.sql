-- active and returning user per market place
with active_users as (
  SELECT
    platform_name,
    buyer_address,
    count(DISTINCT block_timestamp :: date) as active_days
  FROM
    aptos.nft.ez_nft_sales
  GROUP BY
    1,
    2
),
returning_users AS (
  SELECT
    platform_name,
    count(buyer_address) as returning_user_count
  FROM
    active_users
  where
    active_days > 1
  GROUP BY
    1
),
active_user_count as (
  select
    platform_name,
    count(DISTINCT buyer_address) as active_user_count
  from
    aptos.nft.ez_nft_sales
  GROUP BY
    1
)
SELECT
  a.platform_name,
  a.active_user_count,
  coalesce(r.returning_user_count, 0) as returning_user_count
from
  active_user_count a
  LEFT join returning_users r on a.platform_name = r.platform_name
ORDER BY
  1
