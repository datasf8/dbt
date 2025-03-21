{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    all_players as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_all_players
        where play_sk_play <> '-1'
    )

select
    play_sk_play as play_pk_play,
    play_id_play,
    play_code_play,
    play_label_play,
    play_status_play
from all_players
union 
select '-1',Null,Null,Null,Null
