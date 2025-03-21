{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_all_players_status")) }}
from {{ ref("dim_all_players_status") }}
