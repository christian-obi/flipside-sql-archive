with daily_transactions as(
  SELECT
    date_trunc('day', block_timestamp) as transaction_date,
    tx_hash,
    origin_from_address as user
  from
    monad.testnet.fact_event_logs
  where
    contract_address = '0x3019bf1dfb84e5b46ca9d0eec37de08a59a41308'
    and origin_to_address = '0x758d80767a751fc1634f579d76e1ccaab3485c9c'
    and origin_function_signature = '0xadba7a51'
    and topic_0 = '0xcee7d9f7ea527be6fd2ad58f0621348b614e640f77b4e69c5546e2a68ac4db20'
    and tx_succeeded = TRUE
),
first_transaction as(
  SELECT
    user,
    min(transaction_date) as first_seen_date
  FROM
    daily_transactions
  group by
    user
),
address_activity as (
  SELECT
    dt.transaction_date,
    dt.user,
    case
      when dt.transaction_date = ft.first_seen_date then 'new'
      else 'returning'
    end as activity_type
  FROM
    daily_transactions dt
    join first_transaction ft on dt.user = ft.user
)
SELECT
  transaction_date,
  count(
    DISTINCT case
      when activity_type = 'new' then user
    end
  ) as new_user,
  count(
    DISTINCT case
      when activity_type = 'returning' then user
    end
  ) as returning_user,
  count(distinct user) as active_user,
  sum(active_user) over (
    order by
      transaction_date
  ) as cumulative_active_users,
  sum(new_user) over (
    order by
      transaction_date
  ) as cumulative_new_users
FROM
  address_activity
WHERE
  transaction_date >= '2025-02-19'
GROUP BY
  transaction_date
order by
  1 DESC
