{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    * replace (
        all_players_status_id::int as all_players_status_id
    ) rename(all_players_status_id as all_players_status_sk)
from {{ ref("all_players_status") }}
union all
select -1, null, null, null, null, null, null
