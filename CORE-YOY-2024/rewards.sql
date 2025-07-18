--claimedreward =>'0xe33256fedbe96d2ddbd7462c2b1fc3b39e587b388060ce34d1ace27287dad8d3'
with rewards as (
  SELECT
    sum(
      utils.udf_hex_to_int(substr(data, 3, 64)) / pow(10, 18)
    ) as total_reward_amount,
  FROM
    core.core.ez_decoded_event_logs
  WHERE
    contract_address in (
      '0xf5fa1728babc3f8d2a617397fac2696c958c3409',
      '0x0000000000000000000000000000000000001007'
    )
    and topic_0 = '0xe33256fedbe96d2ddbd7462c2b1fc3b39e587b388060ce34d1ace27287dad8d3'
    and block_timestamp >= '2024-01-01'
),
core_price as (
  select
    response as core_price
  from
(
      select
        livequery.live.udf_api(
          'https://api.coingecko.com/api/v3/simple/price?ids=coredaoorg&vs_currencies=usd'
        ) :data:"coredaoorg":usd as response
    )
)
SELECT
  total_reward_amount,
  (total_reward_amount * core_price) as reward_USD
FROM
  rewards,
  core_price
