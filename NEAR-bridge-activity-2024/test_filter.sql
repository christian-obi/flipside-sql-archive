with swap_num as (
  select
    count(*) as num_of_swaps,
    swap_symbol_out --DISTINCT swap_symbol_out
  FROM
    $ query('8514b944-355a-46fa-86df-b08f0dc58bf8')
  GROUP by
    swap_symbol_out
)
SELECT
  *
FROM
  swap_num
WHERE
  num_of_swaps >= 10
  and swap_symbol_out is NOT NULL
ORDER BY
  num_of_swaps DESC
