SELECT
  count(*) as puzzles_solved
from
  aleo.core.fact_blocks
where
  puzzle_reward > 0
