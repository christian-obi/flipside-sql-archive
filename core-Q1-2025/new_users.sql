with daily_transactions as(
  SELECT
    date_trunc('day', block_timestamp) as transaction_date,
    from_address as user
  from
    core.core.fact_transactions
  where
    value_precise > 0
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
  transaction_date > '2025-01-01'
GROUP BY
  transaction_date
order by
  1 DESC
