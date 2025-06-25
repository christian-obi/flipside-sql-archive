select
  datediff(
    day,
    '2025-02-19 15:00:00',
    date_trunc(day, sysdate())
  ) as testnet_days
from
  monad.testnet.dim_contracts
limit
  1
