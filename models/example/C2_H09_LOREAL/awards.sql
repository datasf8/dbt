{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','AWARDS');"
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
    sbaf.backgroundelementid as award_id,
    sbaf.dbt_valid_from as award_start_date,
    nvl(sbaf.dbt_valid_to, '9999-12-31') as award_end_date,
    ed.personal_id as personal_id,
    sbaf.userid as user_id,
    sbaf.institution as award_institution,
    sbaf.description as award_description,
    sbaf.bgorderpos as award_cv_position,
    sbaf.issuedate as award_issue_date

from {{ ref("stg_background_awards_flatten") }} sbaf
join employment_details ed
 on sbaf.userid=ed.user_id
where sbaf.dbt_valid_to is null
