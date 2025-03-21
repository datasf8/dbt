{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook ="UPDATE {{ env_var('DBT_CORE_DB') }}.PMG_CORE_SCH.FACT_YE_FORM YE_F
        SET FYEF_FORMDATA_STATUS_FYEF = '4'
        WHERE (FYEF_EMPLOYEE_PROFILE_KEY_DDEP,FYEF_FORMDATA_ID_FYEF) IN (
        select FYEF_EMPLOYEE_PROFILE_KEY_DDEP,FYEF_FORMDATA_ID_FYEF from {{ env_var('DBT_CORE_DB') }}.PMG_CORE_SCH.FACT_YE_FORM YE_F
        inner join {{ env_var('DBT_STAGING_DB') }}.PMG_STG_SCH.STG_FORM_HEADER_DELETED_FULL_FLATTEN DEL
        on FYEF_EMPLOYEE_PROFILE_KEY_DDEP=FORM_SUBJECT_ID
        and FYEF_FORMDATA_ID_FYEF=FORMDATA_ID);"
    )
}}

with
    fact_ratings as (
        select distinct
            a.formdata_id as fyef_formdata_id_fyef,
            a.emp_id as fyef_employee_profile_key_ddep,
            a.formdata_status as fyef_formdata_status_fyef,
            a.form_title as fyef_form_name_fyef,
            k.form_template_id as fyef_form_template_id_fyef,
            a.pe_formcontent_id as fyef_pe_formcontent_id_fyef,
            case
                when a.pe_pg_rating is not null
                then cast(d.dyer_sk_dyer as char)
                else '-1'
            end as fyef_pe_pg_rating_key_dyer,
            case
                when a.pe_bg_rating is not null
                then cast(e.dyer_sk_dyer as char)
                else '-1'
            end as fyef_pe_bg_rating_key_dyer,
            case
                when a.pe_or_rating is not null
                then cast(f.dyer_sk_dyer as char)
                else '-1'
            end as fyef_pe_or_rating_key_dyer,
            a.pe_lmdate as fyef_pe_lmdate_fyef,
            a.pe_manager_comments as fyef_pe_manager_comments_fyef,
            b.ye_formcontent_id as fyef_ye_formcontent_id_fyef,
            case
                when b.ye_pg_rating is not null
                then cast(g.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_pg_rating_key_dyer,
            case
                when b.ye_bg_rating is not null
                then cast(h.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_bg_rating_key_dyer,
            case
                when b.ye_or_rating is not null
                then cast(i.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_or_rating_key_dyer,
            b.ye_conversation_date as fyef_ye_conversation_date_fyef,
            b.ye_lmdate as fyef_ye_lmdate_fyef,
            b.ye_manager_comments as fyef_ye_manager_comments_fyef,
            b.ye_employee_comments as fyef_ye_employee_comments_fyef,
            c.sgn_formcontent_id as fyef_sgn_formcontent_id_fyef,
            c.sgn_emp_sign as fyef_sgn_emp_sign_fyef,
            c.sgn_manager_sign as fyef_sgn_manager_sign_fyef,
            c.sgn_lmdate as fyef_sgn_lmdate_fyef,
            c.sgn_employee_comments as fyef_sgn_employee_comments_fyef,
            c.sgn_manager_comments as fyef_sgn_manager_comments_fyef,
            c.manager_id as fyef_employee_manager_key_ddep,
            case
                when a.emp_id is not null then 'YES' else 'NO'
            end as fyef_is_form_launched_fyef,
            c.sgn_emp_date as fyef_sgn_employee_date_fyef,
            c.sgn_mgr_date as fyef_sgn_manager_date_fyef,
            j.compl_formcontent_id as fyef_compl_formcontent_id_fyef,
            case
                when j.compl_formcontent_id is not null
                then 'Completed'
                when c.sgn_formcontent_id is not null
                then 'Signatures'
                when b.ye_formcontent_id is not null
                then 'Year End Coversation'
                when a.pe_formcontent_id is not null
                then 'Performance Evaluation'
                else null
            end as fyef_form_status_fyef,
            case
                when a.emp_id = b.sender_id then b.ye_lmdate else null
            end as fyef_ye_employee_lmdate_fyef,
            case
                when c.manager_id = b.sender_id then b.ye_lmdate else null
            end as fyef_ye_manager_lmdate_fyef,
            case
                when a.emp_id = c.sender_id then c.sgn_lmdate else null
            end as fyef_sgn_employee_lmdate_fyef,
            case
                when c.manager_id = c.sender_id then c.sgn_lmdate else null
            end as fyef_sgn_manager_lmdate_fyef

        from (select * from {{ ref("stg_perf_eval_int") }} where dbt_valid_to is null) a

        left outer join
            (select * from {{ ref("stg_year_end_int") }} where dbt_valid_to is null) b
            on a.formdata_id = b.formdata_id

        left outer join
            (select * from {{ ref("stg_sgn_int") }} where dbt_valid_to is null) c
            on a.formdata_id = c.formdata_id
        left outer join
            {{ ref("dim_ye_form_ratings") }} d on a.pe_pg_rating = d.dyer_code_dyer
        left outer join
            {{ ref("dim_ye_form_ratings") }} e on a.pe_bg_rating = e.dyer_code_dyer
        left outer join
            {{ ref("dim_ye_form_ratings") }} f on a.pe_or_rating = f.dyer_code_dyer
        left outer join
            {{ ref("dim_ye_form_ratings") }} g on b.ye_pg_rating = g.dyer_code_dyer
        left outer join
            {{ ref("dim_ye_form_ratings") }} h on b.ye_bg_rating = h.dyer_code_dyer
        left outer join
            {{ ref("dim_ye_form_ratings") }} i on b.ye_or_rating = i.dyer_code_dyer
        left outer join
            (select * from {{ ref("stg_compl_form_int") }} where dbt_valid_to is null) j
            on a.formdata_id = j.formdata_id
        left outer join
            (
                select *
                from {{ ref("stg_form_header_flatten") }}
                where dbt_valid_to is null
            ) k
            on k.formdata_id = a.formdata_id
    ),
    surrogate_key as (
        select p.*,{{ dbt_utils.surrogate_key("FYEF_FORMDATA_ID_FYEF") }} as sk_id
        from fact_ratings p

    )
select
    row_number() over (order by sk_id desc) as fyef_ye_form_key_ddep,
    fyef_formdata_id_fyef,
    fyef_employee_profile_key_ddep,
    nvl(fyef_employee_manager_key_ddep, '-1') as fyef_employee_manager_key_ddep,
    fyef_form_name_fyef,
    fyef_form_template_id_fyef,
    fyef_pe_formcontent_id_fyef,
    cast(fyef_pe_pg_rating_key_dyer as int) as fyef_pe_pg_rating_key_dyer,
    cast(fyef_pe_bg_rating_key_dyer as int) as fyef_pe_bg_rating_key_dyer,
    cast(fyef_pe_or_rating_key_dyer as int) as fyef_pe_or_rating_key_dyer,
    -- FYEF_PE_MANAGER_COMMENTS_FYEF
    case
        when length(fyef_pe_manager_comments_fyef) > 0
        then
            {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.cleanhtml_fct(
                fyef_pe_manager_comments_fyef
            )
        else fyef_pe_manager_comments_fyef
    end as fyef_pe_manager_comments_fyef,
    fyef_pe_lmdate_fyef,
    fyef_ye_formcontent_id_fyef,
    cast(fyef_ye_pg_rating_key_dyer as int) as fyef_ye_pg_rating_key_dyer,
    cast(fyef_ye_bg_rating_key_dyer as int) as fyef_ye_bg_rating_key_dyer,
    cast(fyef_ye_or_rating_key_dyer as int) as fyef_ye_or_rating_key_dyer,
    cast(
        fyef_ye_conversation_date_fyef as timestamp_ntz
    ) as fyef_ye_conversation_date_fyef,
    -- FYEF_YE_MANAGER_COMMENTS_FYEF,
    case
        when length(fyef_ye_manager_comments_fyef) > 0
        then
            {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.cleanhtml_fct(
                fyef_ye_manager_comments_fyef
            )
        else fyef_ye_manager_comments_fyef
    end as fyef_ye_manager_comments_fyef,
    -- FYEF_YE_EMPLOYEE_COMMENTS_FYEF,
    case
        when length(fyef_ye_employee_comments_fyef) > 0
        then
            {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.cleanhtml_fct(
                fyef_ye_employee_comments_fyef
            )
        else fyef_ye_employee_comments_fyef
    end as fyef_ye_employee_comments_fyef,
    fyef_ye_lmdate_fyef,
    fyef_sgn_formcontent_id_fyef,
    cast(fyef_sgn_emp_sign_fyef as boolean) as fyef_sgn_emp_sign_fyef,
    cast(fyef_sgn_manager_sign_fyef as boolean) as fyef_sgn_manager_sign_fyef,
    fyef_sgn_lmdate_fyef,
    -- FYEF_SGN_EMPLOYEE_COMMENTS_FYEF,
    case
        when length(fyef_sgn_employee_comments_fyef) > 0
        then
            {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.cleanhtml_fct(
                fyef_sgn_employee_comments_fyef
            )
        else fyef_sgn_employee_comments_fyef
    end as fyef_sgn_employee_comments_fyef,
    -- FYEF_SGN_MANAGER_COMMENTS_FYEF,
    case
        when length(fyef_sgn_manager_comments_fyef) > 0
        then
            {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.cleanhtml_fct(
                fyef_sgn_manager_comments_fyef
            )
        else fyef_sgn_manager_comments_fyef
    end as fyef_sgn_manager_comments_fyef,
    fyef_is_form_launched_fyef,
    fyef_ye_employee_lmdate_fyef,
    fyef_ye_manager_lmdate_fyef,
    fyef_sgn_employee_lmdate_fyef,
    fyef_sgn_manager_lmdate_fyef,
    fyef_sgn_employee_date_fyef,
    fyef_sgn_manager_date_fyef,
    fyef_form_status_fyef,
    fyef_formdata_status_fyef
from surrogate_key
