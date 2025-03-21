{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','LANGUAGE_MASTERY');"
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
    sblf.backgroundelementid as language_mastery_id,
    sblf.dbt_valid_from as language_mastery_start_date,
    nvl(sblf.dbt_valid_to, '9999-12-31') as language_mastery_end_date,
    ed.personal_id as personal_id,
    sblf.userid as user_id,
    sblf.language as language_id,
    sblf.fluency as fluency_id,
    sblf.bgorderpos as language_mastery_cv_position
from {{ ref("stg_background_languages_flatten") }} sblf
join employment_details ed
 on sblf.userid=ed.user_id
where sblf.dbt_valid_to is null
