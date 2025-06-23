with base as (
SELECT
block_timestamp,
tx_hash,
event_name,
origin_from_address as user_wallet,
decoded_log:amount::int / 1e18 as amount,
sum(amount) over () as total_earned_core
from core.core.ez_decoded_event_logs
where 0=0
and origin_to_address in ('0xee21ab613d30330823d35cf91a84ce964808b83f',  '0x04ea61c431f7934d51fed2acb2c5f942213f8967')--,'0x0000000000000000000000000000000000001007')
and event_name in ('claimedReward')
--and contract_address ='0x0000000000000000000000000000000000001010'
and block_timestamp >= '2024-01-01'
--and origin_from_address =lower('0x79fD22211c01e76f6A0Bbe24D406A80Caf2E7B8C')
and amount> 0
order by 1
)
select
date_trunc('day', block_timestamp) as reward_date,
sum(amount) as amount
from base
group by 1
order by 1 desc
