{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook ="UPDATE {{ env_var('DBT_CORE_DB') }}.PMG_CORE_SCH.FACT_YE_FORM_V2 YE_F
        SET FYEF_FORMDATA_STATUS_FYEF = '4'
        WHERE (FYEF_EMPLOYEE_PROFILE_KEY_DDEP,FYEF_FORMDATA_ID_FYEF) IN (
        select FYEF_EMPLOYEE_PROFILE_KEY_DDEP,FYEF_FORMDATA_ID_FYEF from {{ env_var('DBT_CORE_DB') }}.PMG_CORE_SCH.FACT_YE_FORM_V2 YE_F
        inner join {{ env_var('DBT_STAGING_DB') }}.SDDS_STG_SCH.STG_FORM_HEADER_DELETED_FLATTEN DEL
        on FYEF_EMPLOYEE_ID_FYEF=FORM_SUBJECT_ID
        and FYEF_FORMDATA_ID_FYEF=FORMDATA_ID);"
    )
}}
with
    pe_step as (
        select distinct
            case
                when ratingspgbg.overallcomprating_rating is not null
                then cast(ratingsdimpg.dyer_sk_dyer as char)
                else '-1'
            end as fyef_pe_pg_rating_key_dyer,
            case
                when ratingspgbg.overallobjrating_rating is not null
                then cast(ratingsdimbg.dyer_sk_dyer as char)
                else '-1'
            end as fyef_pe_bg_rating_key_dyer,
            case
                when overallrating.rating is not null
                then cast(ratingsdimoa.dyer_sk_dyer as char)
                else '-1'
            end as fyef_pe_or_rating_key_dyer,
            case
                when ratingsdimpg.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimpg.dyer_code_dyer || ' - ' || ratingsdimpg.dyer_label_dyer
                else ratingsdimpg.dyer_label_dyer
            end pe_pg_rating_lb,
            case
                when ratingsdimbg.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimbg.dyer_code_dyer || ' - ' || ratingsdimbg.dyer_label_dyer
                else ratingsdimbg.dyer_label_dyer
            end pe_bg_rating_lb,
            case
                when ratingsdimoa.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimoa.dyer_code_dyer || ' - ' || ratingsdimoa.dyer_label_dyer
                else ratingsdimoa.dyer_label_dyer
            end pe_oa_rating_lb,
            audittrails.audittraillastmodified fyef_pe_lmdate_fyef,
            pemanagercomment.othersratingcomment_comment
            as fyef_pe_manager_comments_fyef,
            audittrails.formcontentid as fyef_compl_formcontent_id_fyef,
            audittrails.formdataid
        from
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formaudittrails_flatten
                where dbt_valid_to is null
            ) audittrails
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
            ) pemanagercomment
            on audittrails.formdataid = pemanagercomment.formdataid
            and audittrails.formcontentid = pemanagercomment.formcontentid
            and audittrails.formcontentassociatedstepid = '1'
            and pemanagercomment.sectionindex = 14
        inner join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formobjcompsummarysection_flatten
                where dbt_valid_to is null
            ) ratingspgbg
            on audittrails.formdataid = ratingspgbg.formdataid
            and audittrails.formcontentid = ratingspgbg.formcontentid
            and audittrails.formcontentassociatedstepid = '1'
        inner join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formsummarysection_flatten
                where dbt_valid_to is null
            ) overallrating
            on audittrails.formdataid = overallrating.formdataid
            and audittrails.formcontentid = overallrating.formcontentid
            and audittrails.formcontentassociatedstepid = '1'
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimpg
            on ratingspgbg.overallcomprating_rating = ratingsdimpg.dyer_code_dyer
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimbg
            on ratingspgbg.overallobjrating_rating = ratingsdimbg.dyer_code_dyer
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimoa
            on overallrating.rating = ratingsdimoa.dyer_code_dyer
        qualify
            row_number() over (
                partition by audittrails.formdataid
                order by
                    audittrails.audittraillastmodified desc,
                    audittrails.audittrailid desc
            )
            = 1
    ),
    ye_step as (
        select distinct
            case
                when ratingspgbg.overallcomprating_rating is not null
                then cast(ratingsdimpg.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_pg_rating_key_dyer,
            case
                when ratingspgbg.overallobjrating_rating is not null
                then cast(ratingsdimbg.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_bg_rating_key_dyer,
            case
                when overallrating.rating is not null
                then cast(ratingsdimoa.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_or_rating_key_dyer,
            case
                when ratingsdimpg.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimpg.dyer_code_dyer || ' - ' || ratingsdimpg.dyer_label_dyer
                else ratingsdimpg.dyer_label_dyer
            end ye_pg_rating_lb,
            case
                when ratingsdimbg.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimbg.dyer_code_dyer || ' - ' || ratingsdimbg.dyer_label_dyer
                else ratingsdimbg.dyer_label_dyer
            end ye_bg_rating_lb,
            case
                when ratingsdimoa.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimoa.dyer_code_dyer || ' - ' || ratingsdimoa.dyer_label_dyer
                else ratingsdimoa.dyer_label_dyer
            end ye_oa_rating_lb,
            audittrails.audittraillastmodified fyef_ye_lmdate_fyef,
            -- yemanagercomment.othersratingcomment_comment
            -- as fyef_ye_manager_comments_fyef,
            audittrails.formcontentid as fyef_compl_formcontent_id_fyef,
            audittrails.formdataid,
            case
                when conversationdate.customelement_value <> ''
                then cast(conversationdate.customelement_value as timestamp_ntz)
                else null
            end as fyef_ye_conversation_date_fyef
        from
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formaudittrails_flatten
                where dbt_valid_to is null
            ) audittrails
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
            ) conversationdate
            on audittrails.formdataid = conversationdate.formdataid
            and audittrails.formcontentid = conversationdate.formcontentid
            and audittrails.formcontentassociatedstepid = '3'
            and conversationdate.sectionindex = 12
        inner join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formobjcompsummarysection_flatten
                where dbt_valid_to is null
            ) ratingspgbg
            on audittrails.formdataid = ratingspgbg.formdataid
            and audittrails.formcontentid = ratingspgbg.formcontentid
            and audittrails.formcontentassociatedstepid = '3'
        inner join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formsummarysection_flatten
                where dbt_valid_to is null
            ) overallrating
            on audittrails.formdataid = overallrating.formdataid
            and audittrails.formcontentid = overallrating.formcontentid
            and audittrails.formcontentassociatedstepid = '3'
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimpg
            on ratingspgbg.overallcomprating_rating = ratingsdimpg.dyer_code_dyer
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimbg
            on ratingspgbg.overallobjrating_rating = ratingsdimbg.dyer_code_dyer
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimoa
            on overallrating.rating = ratingsdimoa.dyer_code_dyer
        qualify
            row_number() over (
                partition by audittrails.formdataid
                order by
                    audittrails.audittraillastmodified desc,
                    audittrails.audittrailid desc,
                    iff(conversationdate.customelement_value is null, 0, 1) desc
            )
            = 1
    ),
    ye_step_2 as (
        select distinct
            case
                when ratingspgbg.overallcomprating_rating is not null
                then cast(ratingsdimpg.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_pg_rating_key_dyer,
            case
                when ratingspgbg.overallobjrating_rating is not null
                then cast(ratingsdimbg.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_bg_rating_key_dyer,
            case
                when overallrating.rating is not null
                then cast(ratingsdimoa.dyer_sk_dyer as char)
                else '-1'
            end as fyef_ye_or_rating_key_dyer,
            case
                when ratingsdimpg.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimpg.dyer_code_dyer || ' - ' || ratingsdimpg.dyer_label_dyer
                else ratingsdimpg.dyer_label_dyer
            end ye_pg_rating_lb,
            case
                when ratingsdimbg.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimbg.dyer_code_dyer || ' - ' || ratingsdimbg.dyer_label_dyer
                else ratingsdimbg.dyer_label_dyer
            end ye_bg_rating_lb,
            case
                when ratingsdimoa.dyer_code_dyer in (1.0, 2.0, 3.0, 4.0, 5.0)
                then
                    ratingsdimoa.dyer_code_dyer || ' - ' || ratingsdimoa.dyer_label_dyer
                else ratingsdimoa.dyer_label_dyer
            end ye_oa_rating_lb,
            audittrails.audittraillastmodified fyef_ye_lmdate_fyef,
            employee_comment.othersratingcomment_comment
            as fyef_ye_employee_comment_fyef,
            audittrails.formcontentid as fyef_compl_formcontent_id_fyef,
            audittrails.formdataid,
            case
                when conversationdate.customelement_value <> ''
                then cast(conversationdate.customelement_value as timestamp_ntz)
                else null
            end as fyef_ye_conversation_date_fyef
        from
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formaudittrails_flatten
                where dbt_valid_to is null
            ) audittrails
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
            ) employee_comment
            on audittrails.formdataid = employee_comment.formdataid
            and audittrails.formcontentid = employee_comment.formcontentid
            and audittrails.formcontentassociatedstepid = '4'
            and employee_comment.sectionindex = 13
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
            ) conversationdate
            on audittrails.formdataid = conversationdate.formdataid
            and audittrails.formcontentid = conversationdate.formcontentid
            and audittrails.formcontentassociatedstepid = '4'
            and conversationdate.sectionindex = 12
        inner join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formobjcompsummarysection_flatten
                where dbt_valid_to is null
            ) ratingspgbg
            on audittrails.formdataid = ratingspgbg.formdataid
            and audittrails.formcontentid = ratingspgbg.formcontentid
            and audittrails.formcontentassociatedstepid = '4'
        inner join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formsummarysection_flatten
                where dbt_valid_to is null
            ) overallrating
            on audittrails.formdataid = overallrating.formdataid
            and audittrails.formcontentid = overallrating.formcontentid
            and audittrails.formcontentassociatedstepid = '4'
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimpg
            on ratingspgbg.overallcomprating_rating = ratingsdimpg.dyer_code_dyer
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimbg
            on ratingspgbg.overallobjrating_rating = ratingsdimbg.dyer_code_dyer
        inner join
            {{ ref("dim_ye_form_ratings") }} ratingsdimoa
            on overallrating.rating = ratingsdimoa.dyer_code_dyer
        qualify
            row_number() over (
                partition by audittrails.formdataid
                order by
                    audittrails.audittraillastmodified desc,
                    audittrails.audittrailid desc,
                    iff(
                        employee_comment.othersratingcomment_comment is null, 0, 1
                    ) desc,
                    iff(conversationdate.customelement_value is null, 0, 1) desc
            )
            = 1
    ),
    managers_comments as (
        select distinct
            audittrails.audittraillastmodified fyef_ye_lmdate_fyef,
            yemanagercomment.othersratingcomment_comment
            as fyef_ye_manager_comments_fyef,
            yematrix_managercomment.othersratingcomment_comment
            as fyef_ye_matrix_manager_comments_fyef,
            audittrails.formcontentid as fyef_compl_formcontent_id_fyef,
            audittrails.formdataid,
            employee_manager.reem_fk_manager_key_ddep as fyef_ye_manager_id_fyef,
            employee_manager.reem_fk_matrix_manager_key_ddep
            as fyef_ye_matrix_manager_id_fyef,
            audittrails.audittraillastmodified as ye_manager_lmdate,
            audittrails.audittraillastmodified as ye_matrix_manager_lmdate
        from
            (
                select *
                from {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formheader_flatten
                where dbt_valid_to is null
                qualify
                    row_number() over (
                        partition by formsubjectid, formtemplateid
                        order by creationdate desc
                    )
                    = 1
            ) header
        inner join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formaudittrails_flatten
                where dbt_valid_to is null
            ) audittrails
            on audittrails.formdataid = header.formdataid
        inner join
            {{ ref("rel_employee_managers") }} employee_manager
            on header.formsubjectid = employee_manager.reem_employee_profile_key_ddep
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
            ) yemanagercomment
            on audittrails.formdataid = yemanagercomment.formdataid
            and audittrails.formcontentid = yemanagercomment.formcontentid
            and audittrails.formcontentassociatedstepid = '3'
            and yemanagercomment.sectionindex = 14
            and yemanagercomment.othersratingcomment_userid not in (
                employee_manager.reem_employee_profile_key_ddep,
                nvl(employee_manager.reem_fk_matrix_manager_key_ddep, '-1')
            )
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
            ) yematrix_managercomment
            on audittrails.formdataid = yematrix_managercomment.formdataid
            and audittrails.formcontentid = yematrix_managercomment.formcontentid
            and audittrails.formcontentassociatedstepid = '3'
            and yematrix_managercomment.sectionindex = 14
            and employee_manager.reem_fk_matrix_manager_key_ddep
            = yematrix_managercomment.othersratingcomment_userid
        where
            (
                yemanagercomment.sectionindex = 14
                or yematrix_managercomment.sectionindex = 14
            )
        qualify
            row_number() over (
                partition by audittrails.formdataid
                order by
                    audittrails.audittraillastmodified desc,
                    audittrails.audittrailid desc,
                    yemanagercomment.othersratingcomment_comment
            )
            = 1
    ),
    sign_comments as (
        select distinct
            employeecomment.othersratingcomment_comment
            as fyef_sgn_employee_comments_fyef,
            managercomment.othersratingcomment_comment
            as fyef_sgn_manager_comments_fyef,
            audittrails.formcontentid as fyef_compl_formcontent_id_fyef,
            audittrails.formdataid,
            audittrails.audittraillastmodified fyef_sgn_lmdate_fyef,
            employee_manager.reem_employee_profile_key_ddep
            as fyef_sgn_employee_id_fyef,
            employee_manager.reem_fk_manager_key_ddep as fyef_sgn_manager_id_fyef,
            employee_sign.customelement_checked as fyef_employee_signature_fyef,
            manager_sign.customelement_checked as fyef_manager_signature_fyef
        from
            (
                select *
                from {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formheader_flatten
                where dbt_valid_to is null
                qualify
                    row_number() over (
                        partition by formsubjectid, formtemplateid
                        order by creationdate desc
                    )
                    = 1
            ) header
        inner join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formaudittrails_flatten
                where dbt_valid_to is null
            ) audittrails
            on audittrails.formdataid = header.formdataid
        inner join
            {{ ref("rel_employee_managers") }} employee_manager
            on header.formsubjectid = employee_manager.reem_employee_profile_key_ddep
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
                qualify
                    row_number() over (
                        partition by
                            formdataid,
                            formcontentid,
                            sectionindex,
                            othersratingcomment_userid
                        order by dbt_updated_at desc
                    )
                    = 1
            ) employeecomment
            on audittrails.formdataid = employeecomment.formdataid
            and audittrails.formcontentid = employeecomment.formcontentid
            and audittrails.formcontentassociatedstepid = '5'
            and employeecomment.sectionindex = 19
            and employee_manager.reem_employee_profile_key_ddep
            = employeecomment.othersratingcomment_userid
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
                qualify
                    row_number() over (
                        partition by
                            formdataid,
                            formcontentid,
                            sectionindex,
                            othersratingcomment_userid
                        order by dbt_updated_at desc
                    )
                    = 1
            ) managercomment
            on audittrails.formdataid = managercomment.formdataid
            and audittrails.formcontentid = managercomment.formcontentid
            and audittrails.formcontentassociatedstepid = '5'
            and managercomment.sectionindex = 19
            and managercomment.othersratingcomment_userid not in (
                employee_manager.reem_employee_profile_key_ddep,
                nvl(employee_manager.reem_fk_matrix_manager_key_ddep, '-1')
            )
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
                qualify
                    row_number() over (
                        partition by
                            formdataid,
                            formcontentid,
                            sectionindex,
                            customelement_elementkey
                        order by dbt_updated_at desc
                    )
                    = 1
            ) employee_sign
            on audittrails.formdataid = employee_sign.formdataid
            and audittrails.formcontentid = employee_sign.formcontentid
            and audittrails.formcontentassociatedstepid = '5'
            and employee_sign.sectionindex = 19
            and employee_sign.customelement_elementkey = 'EmpSign'
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formcustomsection_flatten
                where dbt_valid_to is null
                qualify
                    row_number() over (
                        partition by
                            formdataid,
                            formcontentid,
                            sectionindex,
                            customelement_elementkey
                        order by dbt_updated_at desc
                    )
                    = 1
            ) manager_sign
            on audittrails.formdataid = manager_sign.formdataid
            and audittrails.formcontentid = manager_sign.formcontentid
            and audittrails.formcontentassociatedstepid = '5'
            and manager_sign.sectionindex = 19
            and manager_sign.customelement_elementkey = 'ManagerSign'
        where (employee_sign.sectionindex = 19 or manager_sign.sectionindex = 19)
        qualify
            row_number() over (
                partition by audittrails.formdataid
                order by
                    audittrails.audittraillastmodified desc,
                    audittrails.audittrailid desc
            )
            = 1
    ),
    completed_step as (
        select
            audittrails.formdataid,
            audittrails.formcontentid as fyef_completed_content_id_fyef,
            audittrails.audittraillastmodified fyef_cmpl_lmdate_fyef
        from
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formaudittrails_flatten
                where dbt_valid_to is null
            ) audittrails
        where audittrails.formcontentassociatedstepid = 'completed'
        qualify
            row_number() over (
                partition by audittrails.formdataid
                order by
                    audittrails.audittraillastmodified desc,
                    audittrails.audittrailid desc
            )
            = 1
    )

select
    header.formdataid as fyef_formdata_id_fyef,
    emplprofile.ddep_employee_profile_sk_ddep as fyef_employee_profile_key_ddep,
    header.formsubjectid as fyef_employee_id_fyef,
    header.formdatastatus as fyef_formdata_status_fyef,
    header.formtitle as fyef_form_name_fyef,
    header.formtemplateid as fyef_form_template_id_fyef,
    pe_step.fyef_pe_pg_rating_key_dyer,
    pe_step.fyef_pe_bg_rating_key_dyer,
    pe_step.fyef_pe_or_rating_key_dyer,
    pe_step.pe_pg_rating_lb,
    pe_step.pe_bg_rating_lb,
    pe_step.pe_oa_rating_lb,
    pe_step.fyef_pe_lmdate_fyef,
    pe_step.fyef_pe_manager_comments_fyef,
    case
        when header.formsubjectid is not null then 'YES' else 'NO'
    end as fyef_is_form_launched_fyef,
    pe_step.fyef_compl_formcontent_id_fyef,
    case
        when ye_step.fyef_ye_pg_rating_key_dyer is not null
        then ye_step.fyef_ye_pg_rating_key_dyer
        else ye_step_2.fyef_ye_pg_rating_key_dyer
    end as fyef_ye_pg_rating_key_dyer,
    case
        when ye_step.fyef_ye_bg_rating_key_dyer is not null
        then ye_step.fyef_ye_bg_rating_key_dyer
        else ye_step_2.fyef_ye_bg_rating_key_dyer
    end as fyef_ye_bg_rating_key_dyer,
    case
        when ye_step.fyef_ye_or_rating_key_dyer is not null
        then ye_step.fyef_ye_or_rating_key_dyer
        else ye_step_2.fyef_ye_or_rating_key_dyer
    end as fyef_ye_or_rating_key_dyer,
    case
        when ye_step.ye_pg_rating_lb is not null
        then ye_step.ye_pg_rating_lb
        else ye_step_2.ye_pg_rating_lb
    end as ye_pg_rating_lb,
    case
        when ye_step.ye_bg_rating_lb is not null
        then ye_step.ye_bg_rating_lb
        else ye_step_2.ye_bg_rating_lb
    end as ye_bg_rating_lb,
    case
        when ye_step.ye_oa_rating_lb is not null
        then ye_step.ye_oa_rating_lb
        else ye_step_2.ye_oa_rating_lb
    end as ye_oa_rating_lb,
    case
        when ye_step.fyef_ye_lmdate_fyef is not null
        then ye_step.fyef_ye_lmdate_fyef
        else ye_step_2.fyef_ye_lmdate_fyef
    end as fyef_ye_lmdate_fyef,
    case
        when ye_step.fyef_compl_formcontent_id_fyef is not null
        then ye_step.fyef_compl_formcontent_id_fyef
        else ye_step_2.fyef_compl_formcontent_id_fyef
    end as fyef_ye_formcontent_id_fyef,
    case
        when ye_step.fyef_ye_conversation_date_fyef is not null
        then ye_step.fyef_ye_conversation_date_fyef
        else ye_step_2.fyef_ye_conversation_date_fyef
    end as fyef_ye_conversation_date_fyef,
    ye_step_2.fyef_ye_employee_comment_fyef,
    ye_step_2.fyef_ye_lmdate_fyef as ye_employee_lmdate,
    managers_comments.ye_manager_lmdate,
    managers_comments.fyef_ye_manager_comments_fyef,
    managers_comments.ye_matrix_manager_lmdate,
    managers_comments.fyef_ye_matrix_manager_comments_fyef,
    managers_comments.fyef_ye_manager_id_fyef,
    managers_comments.fyef_ye_matrix_manager_id_fyef,
    sign_comments.fyef_compl_formcontent_id_fyef as fyef_sgn_formcontent_id_fyef,
    sign_comments.fyef_sgn_employee_comments_fyef,
    sign_comments.fyef_sgn_manager_comments_fyef,
    sign_comments.fyef_employee_signature_fyef,
    sign_comments.fyef_manager_signature_fyef,
    sign_comments.fyef_sgn_employee_id_fyef,
    sign_comments.fyef_sgn_manager_id_fyef,
    sign_comments.fyef_sgn_lmdate_fyef,
    case
        when header.currentstep is not null
        then null
        else completed_step.fyef_completed_content_id_fyef
    end as fyef_completed_content_id_fyef,
    case
        when header.currentstep is not null
        then null
        else completed_step.fyef_cmpl_lmdate_fyef
    end as fyef_cmpl_lmdate_fyef,
    case
        when header.currentstep is not null then header.currentstep else 'Completed'
    end as fyef_form_status_fyef

from
    (
        select *
        from {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_formheader_flatten
        where dbt_valid_to is null
        qualify
            row_number() over (
                partition by formsubjectid, formtemplateid order by creationdate desc
            )
            = 1
    ) header
left outer join pe_step on pe_step.formdataid = header.formdataid
inner join
    {{ ref("dim_employee_profile") }} emplprofile
    on header.formsubjectid = emplprofile.ddep_employee_id_ddep
left outer join ye_step on ye_step.formdataid = header.formdataid
left outer join ye_step_2 on ye_step_2.formdataid = header.formdataid
left outer join managers_comments on managers_comments.formdataid = header.formdataid
left outer join sign_comments on sign_comments.formdataid = header.formdataid
left outer join completed_step on completed_step.formdataid = header.formdataid
where header.formdatastatus <> 4 and pe_step.formdataid is not null
