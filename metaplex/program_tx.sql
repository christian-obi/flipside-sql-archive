WITH program_mappings AS (
  SELECT
    program_id,
    CASE
      program_id
      WHEN 'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk' THEN 'Auction_House'
      WHEN 'neer8g6yJq2mQM6KbnViEDAD4gr3gRZyMMf4F2p3MEh' THEN 'Auctioneer'
      WHEN 'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY' THEN 'Bubblegum'
      WHEN 'Guard1JwRhJkVH6XZhzoYxeBVQe872VH6QggF4BWmS9g' THEN 'Candy_guard'
      WHEN 'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR' THEN 'Candy_machine_v3'
      WHEN 'CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d' THEN 'Core'
      WHEN 'CMAGAKJ67e9hRZgfC5SFTbZH8MgEmtqazKXjmkaJjWTJ' THEN 'Core_candy_guard'
      WHEN 'CMACYFENjoBMHzapRXyo1JZkVS6EtaDDzkjMrmQLvr4J' THEN 'Core_candy_machine'
      WHEN 'gdrpGjVffourzkdDRrQmySw4aTHr8a3xmQzzxSwFD1a' THEN 'Gumdrop'
      WHEN 'hyDQ4Nz1eYyegS6JfenyKwKzYxRsCWCriYSAjtzP4Vg' THEN 'Hydra'
      WHEN '1NSCRfGeyo7wPUazGbaPBUsTM49e1k2aXewHGARfzSo' THEN 'Inscriptions'
      WHEN 'MPL4o4wMzndgh8T1NVDxELQCj5UQfYTYEkabX3wNKtb' THEN 'MPL_hybrid'
      WHEN 'auth9SigNpDKz4sJJ1DfCTuZrZNSAgh9sFD3rboVmgg' THEN 'Token_auth_rules'
      WHEN 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' THEN 'Token_metadata'
      WHEN 'SysExL2WDyJi9aRZrXorrjHJut3JwHQ7R9bTyctbNNG' THEN 'MPL_system_extras'
      WHEN 'TokExjvjJmhKaRBShsBAsbSvEWMA1AgUNK7ps4SAc2p' THEN 'MPL_token_extras'
      ELSE 'others'
    END AS metaplex_program
  FROM
    (
      SELECT
        DISTINCT program_id
      FROM
        solana.core.fact_events
      WHERE
        program_id IN (
          'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk',
          'neer8g6yJq2mQM6KbnViEDAD4gr3gRZyMMf4F2p3MEh',
          'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY',
          'Guard1JwRhJkVH6XZhzoYxeBVQe872VH6QggF4BWmS9g',
          'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR',
          'CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d',
          'CMAGAKJ67e9hRZgfC5SFTbZH8MgEmtqazKXjmkaJjWTJ',
          'CMACYFENjoBMHzapRXyo1JZkVS6EtaDDzkjMrmQLvr4J',
          'gdrpGjVffourzkdDRrQmySw4aTHr8a3xmQzzxSwFD1a',
          'hyDQ4Nz1eYyegS6JfenyKwKzYxRsCWCriYSAjtzP4Vg',
          '1NSCRfGeyo7wPUazGbaPBUsTM49e1k2aXewHGARfzSo',
          'MPL4o4wMzndgh8T1NVDxELQCj5UQfYTYEkabX3wNKtb',
          'auth9SigNpDKz4sJJ1DfCTuZrZNSAgh9sFD3rboVmgg',
          'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s',
          'SysExL2WDyJi9aRZrXorrjHJut3JwHQ7R9bTyctbNNG',
          'TokExjvjJmhKaRBShsBAsbSvEWMA1AgUNK7ps4SAc2p'
        )
    )
),
base_table AS (
  SELECT
    e.program_id,
    m.metaplex_program,
    e.block_timestamp,
    e.tx_id,
    e.signers [0] AS user,
    e.succeeded,
    e.event_type
  FROM
    solana.core.fact_events e
    JOIN program_mappings m ON e.program_id = m.program_id
  WHERE
    e.succeeded = 'TRUE'
    AND e.program_id IN (
      'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk',
      'neer8g6yJq2mQM6KbnViEDAD4gr3gRZyMMf4F2p3MEh',
      'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY',
      'Guard1JwRhJkVH6XZhzoYxeBVQe872VH6QggF4BWmS9g',
      'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR',
      'CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d',
      'CMAGAKJ67e9hRZgfC5SFTbZH8MgEmtqazKXjmkaJjWTJ',
      'CMACYFENjoBMHzapRXyo1JZkVS6EtaDDzkjMrmQLvr4J',
      'gdrpGjVffourzkdDRrQmySw4aTHr8a3xmQzzxSwFD1a',
      'hyDQ4Nz1eYyegS6JfenyKwKzYxRsCWCriYSAjtzP4Vg',
      '1NSCRfGeyo7wPUazGbaPBUsTM49e1k2aXewHGARfzSo',
      'MPL4o4wMzndgh8T1NVDxELQCj5UQfYTYEkabX3wNKtb',
      'auth9SigNpDKz4sJJ1DfCTuZrZNSAgh9sFD3rboVmgg',
      'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s',
      'SysExL2WDyJi9aRZrXorrjHJut3JwHQ7R9bTyctbNNG',
      'TokExjvjJmhKaRBShsBAsbSvEWMA1AgUNK7ps4SAc2p'
    )
)
SELECT
  COUNT(DISTINCT tx_id) AS transactions,
  COUNT(DISTINCT user) AS wallets
FROM
  base_table
