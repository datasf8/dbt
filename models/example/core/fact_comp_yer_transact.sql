{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;         call masking_policy_apply_sp('{{ env_var('DBT_CORE_DB') }}','CMP_CORE_SCH','FACT_COMP_YER_TRANSACT');         USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;         call rls_policy_apply_sp('{{ env_var('DBT_CORE_DB') }}','CMP_CORE_SCH','FACT_COMP_YER_TRANSACT');",
    )
}}
with
    campaign as (
        select
            sycr.user_id,
            employee_id,
            dcam_pk_campaign_dcam,
            dcam_id_campaign_dcam,
            dcam_lb_label_dcam,
            dc.dcam_pk_start_date_dcam as cmpgn_start_date,
            dcam_lb_fg_end_date_dcam,
            rating,
            flag_promotion_key,
            flag_increase_key,
            flag_conformed_to_guidance_key,
            empl_pk_employee_empl,
            annualized_salary,
            new_annualized_salary,
            base_salary_increase_percent,
            salary_increase_delta,
            annualized_salary_increase_merit_guideline_maximum,
            annualized_salary_increase_merit_guideline_low,
            annualized_salary_increase_merit_guideline_high,
            salary_increase_budget_percent,
            salary_increase_budget_amount,
            budget_proration,
            total_proposed_spend,
            dcam_pk_campaign_dcam as campaign_key,
            to_char(dcam_pk_start_date_dcam, 'YYYYMMDD') as dcam_pk_start_date_dcam,
            case
                when salary_increase_budget_amount > 0
                then
                    round(
                        (salary_increase_budget_amount - salary_increase_delta)
                        / salary_increase_budget_amount,
                        2
                    )
                else '0'
            end as fcyt_percent_budget_calculation_fcyt,
            salary_increase_budget_amount
            - salary_increase_delta as fcyt_budget_delta_fcyt,
            rating as rating_r,
            rating_label,
            compa_ratio,
            new_compa_ratio,
            dg.gend_pk_gend as gender_key,
            dpir.dpir_pk_position_in_range_dpir as position_in_range_before_campaign,
            ndpir.dpir_pk_position_in_range_dpir as position_in_range_after_campaign,
            dap.play_pk_play as all_players_key,
            annualized_salary_increase_merit,
            hire_date,
            frequency,
            periodic_base_salary,
            compa_ratio_total_cash,
            new_compa_ratio_total_cash,
            ndpirt.dpir_pk_position_in_range_dpir as new_position_in_range_total_cash,
            dpirt.dpir_pk_position_in_range_dpir as position_in_range_total_cash,
            one_time_bonus,
            guidance_low,
            guidance_high,
            guidance,
            custom_string2 as player_id,
            case
                when flag_promotion_key = 'Y'
                then 'N/A (Promotion)'
                else
                    case
                        when
                            base_salary_increase_percent
                            between guidance_low and guidance_high
                        then 'Aligned'
                        when base_salary_increase_percent is null
                        then null
                        else 'Not Aligned'
                    end
            end as guidelines_aligned,
            position_title,
            form_title,
            form_holder_name,
            currency,
            fixed_pay,
            total_cash_target,
            new_fixed_pay,
            new_total_cash_target,
            sycr.job_code as job_title,
            sycr.comp_plan_owner,
            dcpy.comp_planner_full_name,
            dcpv_comp_plan_sk_ddep,
            comp_plan_eligibility
        from (select * from {{ ref("stg_ye_cpsn_report_int") }}) sycr
        inner join
            {{ ref("dim_employee") }} de
            on sycr.employee_id = de.empl_person_id_external_empl
            and sycr.user_id = de.empl_user_id_empl
        left outer join
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.cmn_stg_sch.stg_per_personal_flatten
                where dbt_valid_to is null
                qualify
                    row_number() over (
                        partition by person_id_external
                        order by start_date desc, last_modified_date_time desc
                    )
                    = 1
            ) sppf
            on sycr.employee_id = sppf.person_id_external
        left outer join
            {{ ref("dim_campaign") }} dc
            on dc.dcam_id_campaign_dcam = sycr.form_template_id
        left outer join {{ ref("dim_gender") }} dg on sycr.gender = dg.gend_code_gend
        left outer join
            {{ ref("dim_position_in_range") }} dpir
            on dpir.dpir_lb_position_in_range_dpir = sycr.position_in_range
        left outer join
            {{ ref("dim_position_in_range") }} ndpir
            on ndpir.dpir_lb_position_in_range_dpir = sycr.new_position_in_range
        left outer join
            {{ ref("dim_all_players") }} dap on sppf.custom_string2 = dap.play_id_play
        left outer join
            {{ ref("dim_position_in_range") }} dpirt
            on dpirt.dpir_lb_position_in_range_dpir = sycr.position_in_range_total_cash
        left outer join
            {{ ref("dim_position_in_range") }} ndpirt
            on ndpirt.dpir_lb_position_in_range_dpir
            = sycr.new_position_in_range_total_cash
        left outer join
            {{ ref("dim_compensation_planner") }} dcpy
            on sycr.comp_plan_owner = dcpy.comp_planner_user_id
    -- and sycr.form_template_id = dcpy.form_template_id
    ),  -- select * from CAMPAIGN;
    remove_duplicates as (
        select *
        from {{ env_var("DBT_STAGING_DB") }}.cmn_stg_sch.stg_emp_job_flatten
        where dbt_valid_to is null
        qualify
            row_number() over (
                partition by user_id, start_date order by seq_number desc
            )
            = 1
    ),
    seq as (
        select user_id, position
        from remove_duplicates
        where position is not null
        qualify row_number() over (partition by user_id order by start_date desc) = 1

    ),  -- select * from seq;
    job_arch as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by user_id, dcam_id_campaign_dcam
                        order by
                            start_date desc, seq_number desc, joar_dt_begin_joar desc
                    -- last_modified_date_time desc
                    ) as rk_je,
                    *
                from
                    (
                        select distinct
                            cn.*,
                            job_code,
                            job_entry_date,
                            joar_pk_joar,
                            job_entry_date as job_seniority_date,
                            pay_grade,
                            custom_string4,
                            custom_string12,
                            custom_string31,
                            contract_type,
                            label as emp_status,
                            start_date,
                            seq_number,
                            last_modified_date_time,
                            end_date,
                            round(
                                months_between(
                                    dcam_lb_fg_end_date_dcam, job_seniority_date
                                )
                            ) as job_seniority_mth,
                            position,
                            country_of_company,
                            joar_dt_begin_joar
                        from
                            (
                                select *
                                from
                                    {{ env_var("DBT_STAGING_DB") }}.cmn_stg_sch.stg_emp_job_flatten
                                where dbt_valid_to is null
                            ) sejf
                        inner join
                            campaign cn
                            on sejf.user_id = cn.user_id
                            and start_date <= dcam_lb_fg_end_date_dcam
                            and iff(end_date = '1978-01-11', '9999-12-31', end_date)
                            >= cmpgn_start_date
                        left join
                            (
                                select
                                    *,
                                    iff(
                                        joar_dt_end_joar = '1978-01-11',
                                        '9999-12-31',
                                        joar_dt_end_joar
                                    ) calc_joar_dt_end_joar
                                from {{ ref("dim_job_architecture") }}
                            ) dja
                            on sejf.job_code = dja.joar_job_role_code_joar
                            and joar_dt_begin_joar <= dcam_lb_fg_end_date_dcam
                            and calc_joar_dt_end_joar >= cmpgn_start_date
                        left join
                            (
                                select *
                                from
                                    {{ env_var("DBT_STAGING_DB") }}.cmn_stg_sch.stg_pick_list_flatten
                                where dbt_valid_to is null
                            ) splf
                            on sejf.empl_status = splf.id
                    )
            )
        where rk_je = 1

    ),  -- select * from JOB_ARCH;
    job_arch_seq as (
        select nvl(job_arch.position, seq.position) as position1, job_arch.*
        from job_arch
        inner join seq on job_arch.user_id = seq.user_id
    ),  -- select * from job_arch_seq ;
    dim_org as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by user_id, dcam_id_campaign_dcam
                        order by orgn_dt_begin_orgn desc
                    ) as rk_do,
                    *
                from
                    (
                        select distinct
                            user_id,
                            -- nvl(position, seq_position) as position,
                            position1 as position,
                            orgn_pk_organisation_orgn,
                            cust_operationalarea,
                            cust_scopetype,
                            orgn_dt_begin_orgn,
                            dcam_id_campaign_dcam,
                            spf.job_level,
                            spf.cust_key_position_level
                        from job_arch_seq cn
                        inner join
                            (
                                select
                                    *,
                                    iff(
                                        orgn_dt_end_orgn = '1978-01-11',
                                        '9999-12-31',
                                        orgn_dt_end_orgn
                                    ) calc_orgn_dt_end_orgn
                                from {{ ref("dim_organization") }} do
                            ) dorg
                            on cn.position1 = dorg.orgn_position_code_orgn
                            and orgn_dt_begin_orgn <= dcam_lb_fg_end_date_dcam
                            and calc_orgn_dt_end_orgn >= cmpgn_start_date
                        left outer join
                            (
                                select
                                    *,
                                    iff(
                                        effective_end_date = '1978-01-11',
                                        '9999-12-31',
                                        effective_end_date
                                    ) calc_effective_end_date
                                from
                                    {{ env_var("DBT_STAGING_DB") }}.cmn_stg_sch.stg_position_flatten
                            ) spf
                            on cn.position1 = spf.code
                            and spf.effective_start_date = dorg.orgn_dt_begin_orgn
                    -- and effective_start_date <= dcam_lb_fg_end_date_dcam
                    -- and calc_effective_end_date >= cmpgn_start_date
                    )
            )
        where rk_do = 1
    ),  -- select * from DIM_ORG;
    dim_org_v1 as (
        select
            organization_sk,
            dov.cost_center_code,
            organization_start_date,
            organization_end_date,
            job_start_date,
            job_end_date,
            dj.user_id,
            dcam_id_campaign_dcam,
            dcam_lb_fg_end_date_dcam,
            cmpgn_start_date
        from campaign cn
        inner join
            {{ env_var("DBT_PUB_DB") }}.cmn_pub_sch.dim_job dj
            on dj.user_id = cn.user_id
            and job_start_date <= dcam_lb_fg_end_date_dcam
            and job_end_date >= cmpgn_start_date
        inner join
            {{ env_var("DBT_PUB_DB") }}.cmn_pub_sch.dim_organization_v1 dov
            on dov.cost_center_code = dj.cost_center_code
            and organization_start_date <= cn.dcam_lb_fg_end_date_dcam
            and organization_end_date >= cmpgn_start_date
        qualify
            row_number() over (
                partition by dj.user_id, dcam_id_campaign_dcam
                order by job_end_date desc, organization_end_date desc
            )
            = 1

    ),  -- SELECT * FROM DIM_ORG_V1  where user_id ='00460908';
    group_sen as (

        select distinct
            cn.*,
            seef.user_id as user_id_g,
            seef.person_id_external,
            custom_date2 as group_seniority_date,
            round(
                months_between(dcam_lb_fg_end_date_dcam, group_seniority_date)
            ) as group_seniority_mth
        from
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.cmn_stg_sch.stg_emp_employment_flatten
                where dbt_valid_to is null and assignment_class = 'ST'
                qualify
                    row_number() over (
                        partition by person_id_external
                        order by start_date desc, last_modified_date_time desc
                    )
                    = 1
            ) seef
        inner join
            campaign cn
            -- on seef.user_id = cn.user_id
            on seef.person_id_external = cn.employee_id
            and group_seniority_date <= dcam_lb_fg_end_date_dcam

    ),  -- select * from GROUP_SEN;
    group_sen_no_filter as (
        select distinct
            cn.*,
            seef.user_id as user_id_g,
            seef.person_id_external,
            -- custom_date2 as group_seniority_date,
            custom_date2 as group_seniority_date,
            round(
                months_between(dcam_lb_fg_end_date_dcam, group_seniority_date)
            ) as group_seniority_mth_no_filter
        from
            (
                select *
                from
                    {{ env_var("DBT_STAGING_DB") }}.cmn_stg_sch.stg_emp_employment_flatten
                where dbt_valid_to is null  -- and PERSON_ID_EXTERNAL='70499063'
                -- and assignment_class = 'ST'
                qualify
                    row_number() over (
                        partition by person_id_external
                        order by start_date desc, last_modified_date_time desc
                    )
                    = 1
            ) seef
        inner join
            campaign cn
            -- on seef.user_id = cn.user_id
            on seef.person_id_external = cn.employee_id
            and group_seniority_date <= dcam_lb_fg_end_date_dcam
    ),  -- select * from group_sen_no_filter where PERSON_ID_EXTERNAL='70316951';
    age as (
        select round(months_between(dcam_lb_fg_end_date_dcam, dob)) as age_mth, *
        from
            (
                select empl_user_id_empl as user_id_a, empl_date_of_birth_empl as dob
                from {{ ref("dim_employee") }}
            ) dee
        inner join campaign cn on dee.user_id_a = cn.user_id

    ),  -- select * from AGE;
    ratings as (
        select *
        from
            (

                select
                    split_part(dyer_code_dyer, '.', 1) as dyer_code_dyer, dyer_pk_dyer
                from {{ ref("dim_ratings") }}

            ) dr

        inner join campaign cn on dr.dyer_code_dyer = cn.rating
    ),  -- select * from RATINGS;
    hr_manager as (
        select empm_pk_hr_id_empm, empm_pk_user_id_empl, empm_man_type_rel
        from {{ ref("dim_employee_manager_hr") }}
        where empm_man_type_rel = 'HR Manager' and empm_dt_begin_rel <= current_date()
        qualify
            row_number() over (
                partition by empm_pk_user_id_empl order by empm_dt_begin_rel desc
            )
            = 1
    ),
    range_job as

    (select * from {{ ref("dim_range") }} where range_type = 'Job'),
    range_group as

    (select * from {{ ref("dim_range") }} where range_type = 'Group'),
    range_age as

    (select * from {{ ref("dim_range") }} where range_type = 'Age'),
    rem as

    (
        select *
        from campaign cn
        left outer join
            {{ ref("dim_flag_promotion") }} dfp
            on cn.flag_promotion_key = dfp.dfpr_cd_fg_promotion_dfpr
        left outer join
            {{ ref("dim_flag_increase") }} dfi
            on cn.flag_increase_key = dfi.dfin_cd_fg_increase_dfin
        left outer join
            {{ ref("dim_flag_conformed_to_guidance") }} dfctg
            on dfctg.dfcg_cd_fg_conformed_to_guidance_dfcg
            = cn.flag_conformed_to_guidance_key

    ),  -- select * from REM;
    ethnicity as (
        select personidexternal, sk_ethnicity_id
        from
            (
                select personidexternal, customstring12, country
                from
                    {{ env_var("DBT_STAGING_DB") }}.sdds_stg_sch.stg_per_global_info_usa_flatten
                where dbt_valid_to is null and startdate <= current_date()
                qualify
                    row_number() over (
                        partition by personidexternal order by startdate desc
                    )
                    = 1
            )
        join
            {{ env_var("DBT_SDDS_DB") }}.{{ env_var("DBT_SDDS_PERSON_SCH") }}.ethnicity
            on customstring12 = ethnicity_id
            and country = country_code
    )
select distinct
    nvl(rem.campaign_key, '-1') as fcyt_fk_campaign_dcam,
    (
        rem.empl_pk_employee_empl || (to_char(rem.dcam_lb_fg_end_date_dcam, 'YYYYMM'))
    )::number(38, 0) date_employee_key,
    rem.dcam_lb_fg_end_date_dcam as campaign_end_date,
    nvl(rem.employee_id, '-1') as employee_id,
    nvl(rem.user_id, '-1') as user_id,
    nvl(rem.empl_pk_employee_empl, '-1') as fcyt_fk_employee_empl,
    nvl(joar_pk_joar, '-1') as fcyt_fk_job_architecture_orgn,
    nvl(orgn_pk_organisation_orgn, '-1') as fcyt_fk_organization_orgn,
    nvl(rem.gender_key, '-1') as fcyt_pk_gender_fcyt,
    nvl(rem.all_players_key, '-1') as fcyt_fk_all_players_key_dalp,
    nvl(dfpr_pk_fg_promotion_dfpr, '-1') as fcyt_fk_fg_promotion_dfpr,
    nvl(dfin_pk_fg_increase_dfin, '-1') as fcyt_fk_fg_increase_dfin,
    nvl(
        dfcg_pk_fg_conformed_to_guidance_dfcg, '-1'
    ) as fcyt_fk_fcyt_fg_conformed_to_guidance_dfcg,
    rem.annualized_salary as fcyt_uc_current_base_salary_fcyt,
    rem.new_annualized_salary as fcyt_uc_new_base_salary_proposal_fcyt,
    rem.base_salary_increase_percent as fcyt_percent_increase_delta_fcyt,
    rem.salary_increase_delta as fcyt_uc_increase_delta_fcyt,
    rem.annualized_salary_increase_merit_guideline_maximum
    as fcyt_uc_increase_merit_guideline_max_fcyt,
    rem.annualized_salary_increase_merit_guideline_low
    as fcyt_uc_increase_merit_guideline_low_fcyt,
    rem.annualized_salary_increase_merit_guideline_high
    as fcyt_uc_increase_merit_guideline_high_fcyt,
    rem.salary_increase_budget_percent as fcyt_budget_percent_salary_increase_fyct,
    rem.salary_increase_budget_amount as fcyt_budget_amount_salary_increase_fyct,
    rem.budget_proration as fcyt_budget_proration_fcyt,
    rem.total_proposed_spend as fcyt_budget_total_proposed_spend_fyct,
    rem.dcam_pk_start_date_dcam as fcyt_fk_date,
    rem.fcyt_percent_budget_calculation_fcyt as fcyt_percent_budget_calculation_fcyt,
    rem.fcyt_budget_delta_fcyt as fcyt_budget_delta_fcyt,
    nvl(dyer_pk_dyer, -1) as fyct_fk_ratings_dyer,
    rem.compa_ratio as fcyt_compa_ratio_fcyt,
    rem.new_compa_ratio as fcyt_new_compa_ratio_fcyt,
    nvl(rj.range_code, -1) as fahc_job_seniority_range_code_fahc,
    nvl(
        ifnull(rg.range_code, rg_no_filter.range_code), -1
    ) as fahc_group_seniority_range_code_fahc,
    nvl(ra.range_code, -1) as fahc_age_range_code_fahc,
    nvl(
        rem.position_in_range_before_campaign, '-1'
    ) as fcyt_pk_position_in_range_bc_dpir,
    nvl(
        rem.position_in_range_after_campaign, '-1'
    ) as fcyt_pk_position_in_range_ac_dpir,
    nvl(dlpg.lpgr_pk_lpgr, '-1') as fcyt_pk_local_pay_grade_lpgr,
    nvl(dgpg.gpgr_pk_gpgr, '-1') as fcyt_pk_global_pay_grade_gpgr,
    nvl(deg.emgp_pk_emgp, '-1') as fcyt_pk_employee_group_emgp,
    nvl(des.emsg_pk_emsg, '-1') as fcyt_pk_employee_subgroup_emsg,
    nvl(dhh.hoho_pk_hoho, '-1') as fcyt_pk_home_host_hoho,
    nvl(doa.opar_pk_opar, '-1') as fcyt_pk_operational_area_opar,
    nvl(dst.scot_pk_scot, '-1') as fcyt_pk_scope_type_scot,
    nvl(dct.coty_pk_coty, '-1') as fcyt_pk_contract_type_coty,
    rem.annualized_salary_increase_merit as fcyt_annualized_salary_increase_merit_fcyt,
    rem.compa_ratio_total_cash as fcyt_compa_ratio_total_cash_fcyt,
    rem.new_compa_ratio_total_cash as fcyt_new_compa_ratio_total_cash_fcyt,
    nvl(
        rem.new_position_in_range_total_cash, '-1'
    ) as fcyt_pk_new_position_in_range_total_cash_fcyt,
    rem.position_in_range_total_cash as fcyt_pk_position_in_range_total_cash_fcyt,
    rem.hire_date as fcyt_hire_date_fcyt,
    nvl(emp_status, '-1') as fcyt_empl_status_fcyt,
    nvl(job_entry_date, '-1') as fcyt_job_entry_date_orgn,
    rem.frequency as fcyt_frequency_fcyt,
    rem.periodic_base_salary as fcyt_periodic_base_salary_fcyt,
    rem.one_time_bonus as fcyt_one_time_bonus_fcyt,
    rem.guidance_low as fcyt_guidance_low_fcyt,
    rem.guidance_high as fcyt_guidance_high_fcyt,
    rem.guidance as fcyt_guidance_fcyt,
    nvl(sk_ethnicity_id, '-1') as fcyt_pk_ethnicity_ecty,
    nvl(empm_pk_hr_id_empm, '-1') as fcyt_sk_hr_id_empm,
    rem.guidelines_aligned as fcyt_guidelines_aligned_fcyt,
    nvl(dejl.jlvl_pk_jlvl, '-1') as fcyt_pk_job_level_jlvl,
    nvl(dkp.kpos_pk_kpos, '-1') as fcyt_pk_kpos,
    rem.position_title as fcyt_position_title_fcyt,
    rem.form_title as fcyt_form_title_fcyt,
    rem.form_holder_name as fcyt_form_holder_name_fcyt,
    rem.currency as fcyt_currency_fcyt,
    rem.fixed_pay as fcyt_fixed_pay_fcyt,
    rem.total_cash_target as fcyt_total_cash_target_fcyt,
    rem.new_fixed_pay as fcyt_new_fixed_pay_fcyt,
    rem.new_total_cash_target as fcyt_new_total_cash_target_fcyt,
    rem.job_title as fcyt_job_title_fcyt,
    nvl(dov1.organization_sk, '-1') as fcyt_fk_organization_v1_sk_orgn,
    rem.comp_plan_owner as fcyt_comp_plan_owner_fcyt,
    rem.comp_planner_full_name as fcyt_compensation_planner_empl,
    nvl(rem.dcpv_comp_plan_sk_ddep, '-1') as fcyt_comp_plan_sk_dcpv,
    rem.comp_plan_eligibility as fcyt_comp_plan_eligibility_fcyt
from rem
left outer join
    job_arch
    on rem.user_id = job_arch.user_id
    and rem.dcam_id_campaign_dcam = job_arch.dcam_id_campaign_dcam
left outer join
    dim_org
    on rem.user_id = dim_org.user_id
    and rem.dcam_id_campaign_dcam = dim_org.dcam_id_campaign_dcam
-- inner join dim_org on rem.user_id = dim_org.user_id
-- STRY0195986
left outer join
    group_sen
    on rem.user_id = group_sen.user_id
    and rem.dcam_id_campaign_dcam = group_sen.dcam_id_campaign_dcam
left outer join
    group_sen_no_filter
    on rem.user_id = group_sen_no_filter.user_id
    and rem.dcam_id_campaign_dcam = group_sen_no_filter.dcam_id_campaign_dcam
left outer join
    age
    on rem.user_id = age.user_id_a
    and rem.dcam_id_campaign_dcam = age.dcam_id_campaign_dcam
left outer join
    ratings
    on rem.user_id = ratings.user_id
    and rem.dcam_id_campaign_dcam = ratings.dcam_id_campaign_dcam
left outer join range_job rj on job_seniority_mth between rj.range_inf and rj.range_sup
left outer join
    range_group rg on group_seniority_mth between rg.range_inf and rg.range_sup
left outer join
    range_group rg_no_filter
    on group_seniority_mth_no_filter
    between rg_no_filter.range_inf and rg_no_filter.range_sup
left outer join range_age ra on age_mth between ra.range_inf and ra.range_sup
left outer join
    {{ ref("dim_local_pay_grade") }} dlpg
    on (job_arch.country_of_company || job_arch.pay_grade) = dlpg.lpgr_code_lpgr
left outer join
    {{ ref("dim_global_pay_grade") }} dgpg on job_arch.pay_grade = dgpg.gpgr_code_gpgr
left outer join
    {{ ref("dim_employee_group") }} deg on job_arch.custom_string4 = deg.emgp_code_emgp
left outer join
    (
        select *
        from {{ ref("dim_employee_subgroup") }}
        qualify
            row_number() over (
                partition by emsg_code_emsg order by emsg_effectivestartdate_emsg desc
            )
            = 1
    ) des
    on job_arch.custom_string31 = des.emsg_code_emsg
left outer join
    {{ ref("dim_home_host") }} dhh on job_arch.custom_string12 = dhh.hoho_id_hoho
left outer join
    {{ ref("dim_operational_area") }} doa
    on dim_org.cust_operationalarea = doa.opar_code_opar
left outer join
    {{ ref("dim_scope_type") }} dst on dim_org.cust_scopetype = dst.scot_code_scot
left outer join
    {{ ref("dim_contract_type") }} dct on job_arch.contract_type = dct.coty_id_coty
left outer join ethnicity on rem.employee_id = ethnicity.personidexternal
left outer join hr_manager hm on hm.empm_pk_user_id_empl = rem.empl_pk_employee_empl
left outer join
    {{ ref("dim_job_level") }} dejl on dim_org.job_level = dejl.jlvl_code_jlvl
left outer join
    {{ ref("dim_key_position") }} dkp
    on dim_org.cust_key_position_level = dkp.kpos_code_kpos
left join
    dim_org_v1 dov1
    on rem.user_id = dov1.user_id
    and rem.dcam_id_campaign_dcam = dov1.dcam_id_campaign_dcam
left join
    (
        select dcam_id_campaign_dcam, pec.*
        from
            hrdp_sdds_{{ env_var("DBT_ENV_NAME") }}_db.btdp_ds_c2_h15_rewards_information_eu_{{ env_var("DBT_ENV_NAME") }}_private.employee_compensation pec
        left join
            {{ ref("dim_campaign") }} dc
            on employee_compensation_information_start_date
            <= dc.dcam_lb_fg_end_date_dcam
            and employee_compensation_information_end_date >= dc.dcam_pk_start_date_dcam
        where employee_compensation_information_start_date <= current_date()
        qualify
            row_number() over (
                partition by user_id, dcam_id_campaign_dcam
                order by employee_compensation_information_start_date desc
            )
            = 1
    ) ec
    on rem.user_id = ec.user_id
    and rem.dcam_id_campaign_dcam = ec.dcam_id_campaign_dcam
left join
    hrdp_sdds_{{ env_var("DBT_ENV_NAME") }}_db.btdp_ds_c1_h15_rewards_information_eu_{{ env_var("DBT_ENV_NAME") }}_private.compensation_plan_eligibility cpe
    on ec.compensation_plan_eligibility_id = cpe.compensation_plan_eligibility_id
where
    (compensation_plan_eligibility_code <> 'N'
   or compensation_plan_eligibility_code is null)
