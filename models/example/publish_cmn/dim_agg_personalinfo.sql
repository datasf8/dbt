{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    legal_gender_v1 as (
        select
            legal_gender_id,
            legal_gender_code,
            legal_gender_name_en,
            legal_gender_status
        from {{ ref("legal_gender_v1") }}
        where legal_gender_start_date <= current_date()
        qualify
            row_number() over (
                partition by legal_gender_code order by legal_gender_start_date desc
            )
            = 1
        union all
        select null, null, null, null
    ),
    all_players_status_v1 as (
        select
            all_players_status_id,
            all_players_status_code,
            all_players_status_name_en,
            all_players_status_status
        from {{ ref("all_players_status_v1") }}
        where all_players_status_start_date <= current_date()
        qualify
            row_number() over (
                partition by all_players_status_code
                order by all_players_status_start_date desc
            )
            = 1
        union all
        select null, null, null, null
    )
select
    hash(legal_gender_id, all_players_status_id) as agg_personalinfo_sk,
    lg.*,
    iff(
        legal_gender_name_en not in ('Male', 'Female'), 'Other', legal_gender_name_en
    ) as legal_gender_name_label,
    ap.*,
    nvl(all_players_status_name_en, 'Player') as all_players_status_name_label
from legal_gender_v1 lg
join all_players_status_v1 ap
