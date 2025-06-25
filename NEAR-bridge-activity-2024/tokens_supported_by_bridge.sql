SELECT
  platform as bridges,
  count(distinct symbol) as number_of_tokens
from
  near.defi.ez_bridge_activity
GROUP BY
  1
ORDER BY
  2 DESC
