{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','EDUCATION');"
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
    sbef.backgroundelementid as education_id,
    sbef.dbt_valid_from as education_start_date,
    nvl(sbef.dbt_valid_to, '9999-12-31') as education_end_date,
    ed.personal_id as personal_id,
    sbef.userid as user_id,
    sbef.country as iso_country_id,
    sbef.year as year,
    sbef.comments as comments,
    sbef.degree as degree,
    sbef.internationalprogram as international_program,
    sbef.degree_type as degree_type_id,
    sbef.school as school,
    sbef.bgorderpos as education_cv_position,
    sbef.school_new as school_new_id

from {{ ref("stg_background_education_flatten") }} sbef
join employment_details ed
 on sbef.userid=ed.user_id
where sbef.dbt_valid_to is null
