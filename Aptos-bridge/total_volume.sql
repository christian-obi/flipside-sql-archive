SELECT
  sum(amount_in_usd) as total_volume_bridged_usd,
  count(distinct sender) as users
FROM
  aptos.defi.ez_bridge_activity
