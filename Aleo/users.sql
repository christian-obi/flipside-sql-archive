--idea from Azin
with t1 as (
  SELECT
    case
      when inputs [0] ['type'] = 'private'
      OR inputs [1] ['type'] = 'private'
      OR inputs [3] ['type'] = 'private' then 'private'
      else 'public'
    end as typee,
    CASE
      when inputs [0] ['value'] LIKE '%aleo%' then inputs [0] ['value']
      when inputs [1] ['value'] LIKE '%aleo%' then inputs [1] ['value']
      when inputs [2] ['value'] LIKE '%aleo%' then inputs [2] ['value']
      when inputs [3] ['value'] LIKE '%aleo%' then inputs [3] ['value']
      when inputs [4] ['value'] LIKE '%aleo%' then inputs [4] ['value']
      when inputs [5] ['value'] like '%aleo%' then inputs [5] ['value']
    end as user,
    *
  from
    aleo.core.fact_transitions
  WHERE
    succeeded = 'TRUE'
    and typee = 'public'
),
users as(
  SELECT
    min(block_timestamp :: date) as date,
    user
  from
    t1
  GROUP BY
    2
)
SELECT
  date,
  count(*) as new_users,
  sum(new_users) over (
    ORDER BY
      date
  ) as total_users
from
  users
GROUP by
  1
ORDER by
  1
