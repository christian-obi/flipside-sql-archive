SELECT
  date_trunc('day', block_timestamp) as day,
  count(block_id) as total_blocks,
  count(
    case
      when puzzle_reward > 0 then 1
    end
  ) as puzzles_solved,
  sum(total_blocks) over (
    order by
      day
  ) as cumulative_total_blocks,
  sum(puzzles_solved) over (
    order by
      day
  ) as cumulative_puzzle_rewards
FROM
  aleo.core.fact_blocks
group by
  1
order by
  1
