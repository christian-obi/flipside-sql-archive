--G7DEzmfEYytqXZEfYFDtjygtdtxNeUzeHm3emyYUMLLv
SELECT
  tx_to,
  to_varchar(SUM(amount), '999,999,999.00') AS airdrop_amount
FROM
  (
    SELECT
      tx_to,
      amount
    FROM
      solana.core.fact_transfers
    WHERE
      tx_from = 'G7DEzmfEYytqXZEfYFDtjygtdtxNeUzeHm3emyYUMLLv'
      AND TX_TO != 'G7DEzmfEYytqXZEfYFDtjygtdtxNeUzeHm3emyYUMLLv'
      AND mint = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
  ) AS filtered_data
GROUP BY
  tx_to
ORDER BY
  SUM(amount) DESC
LIMIT
  200;
