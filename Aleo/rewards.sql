SELECT
  block_timestamp :: date as date,
  sum(block_reward / pow(10, 6)) as block_reward_adj,
  sum(puzzle_reward / pow(10, 6)) as puzzle_reward_adj,
  sum(block_reward_adj) over (
    ORDER BY
      date
  ) as total_block_reward,
  sum(puzzle_reward_adj) over (
    ORDER by
      date
  ) as total_puzzle_reward,
  count(DISTINCT block_hash) as no_of_blocks,
  sum(no_of_blocks) over (
    ORDER BY
      date
  ) as total_block_number,
  max(puzzle_reward / pow(10, 6)) * 3 / 2 as coinbase_reward,
  sum(puzzle_reward / pow(10, 6)) * 3 / 2 as coinbase_reward_total
FROM
  aleo.core.fact_blocks
group by
  1
order by
  1
