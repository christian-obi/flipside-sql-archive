WITH daily_transactions AS (
    SELECT 
        DATE(block_timestamp) AS tx_date,
        from_address AS address,
        COUNT(tx_hash) AS daily_tx_count,
        SUM(value_precise) AS daily_value
    FROM core.core.fact_transactions
    where block_timestamp >= '2024-01-01'
    GROUP BY tx_date, address
),
filtered_addresses AS (
    SELECT 
        address
    FROM daily_transactions
    WHERE daily_tx_count <= 200 -- Filter out bots (addresses with >200 transactions per day)
      AND daily_value >= 1      -- Filter out spams (addresses with cumulative value < 1)
),
daily_active_addresses AS (
    SELECT 
        tx_date,
        COUNT(DISTINCT address) AS daily_active_addresses
    FROM daily_transactions
    WHERE address IN (SELECT address FROM filtered_addresses)
    GROUP BY tx_date
),
daa_with_change AS (
    SELECT 
        tx_date,
        daily_active_addresses,
        LAG(daily_active_addresses) OVER (ORDER BY tx_date) AS previous_day_daa,
        CASE 
            WHEN LAG(daily_active_addresses) OVER (ORDER BY tx_date) IS NULL THEN NULL
            ELSE ROUND(((daily_active_addresses - LAG(daily_active_addresses) OVER (ORDER BY tx_date)) 
                        / LAG(daily_active_addresses) OVER (ORDER BY tx_date)) * 100, 2)
        END AS percentage_change
    FROM daily_active_addresses
)
SELECT 
    tx_date,
    daily_active_addresses,
    percentage_change
FROM daa_with_change
ORDER BY tx_date DESC
