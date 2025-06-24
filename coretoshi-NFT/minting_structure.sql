with tab1 as (
  select
    'Team Mint' as "Distribution Type",
    'Free' as "Price",
    'December 12, 2024, 10:00 PM UTC+8' as "Minting Phases"
  from
    core.nft.ez_nft_transfers
  limit
    1
), tab2 as (
  select
    'OG Presale' as "Distribution Type",
    '10 $CORE' as "Price",
    'December 13, 2024, 10:30 PM UTC+8' as "Minting Phases"
  from
    core.nft.ez_nft_transfers
  limit
    1
), tab3 as (
  select
    'Public Sale' as "Distribution Type",
    '20 $CORE' as "Price",
    'December 14, 2024, 10:45 PM UTC+8' as "Minting Phases"
  from
    core.nft.ez_nft_transfers
  limit
    1
)
select
  *
from
  tab1
union
all
select
  *
from
  tab2
union
all
select
  *
from
  tab3
