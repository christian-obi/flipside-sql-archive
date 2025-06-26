-- total volume bridged USD by platform
SELECT
  platform,
  sum(amount_in_usd) as total_bridged_usd,
  sum(
    case
      when direction = 'outbound' then - amount_in_usd
    end
  ) as bridge_out,
  sum(
    CASE
      when direction = 'inbound' then amount_in_usd
    end
  ) as bridge_in
from
  aptos.defi.ez_bridge_activity
group by
  1
