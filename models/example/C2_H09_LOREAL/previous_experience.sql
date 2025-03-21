{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','PREVIOUS_EXPERIENCE');"
    )
}}
with employment_details as (
select user_id, personal_id from {{ ref("employment_details") }}
qualify
                    row_number() over (
                        partition by user_id
                        order by employment_details_start_date desc
                    )
                    = 1
)
select
    sbof.backgroundelementid as previous_experience_id,
    sbof.dbt_valid_from as previous_experience_start_date,
    nvl(sbof.dbt_valid_to, '9999-12-31') as previous_experience_end_date,
    ed.personal_id as personal_id,
    sbof.userid as user_id,
    sbof.startdate as experience_start_date,
    sbof.enddate as experience_end_date,
    sbof.country as iso_country_id,
    sbof.company as company,
    sbof.job as job,
    sbof.bgorderpos as previous_experience_cv_position
from {{ ref("stg_background_outsideworkexperience_flatten") }} sbof
join employment_details ed
 on sbof.userid=ed.user_id
where sbof.dbt_valid_to is null
