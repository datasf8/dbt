{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;         call masking_policy_apply_sp('{{ env_var('DBT_CORE_DB') }}','CMP_CORE_SCH','FACT_VARIABLE_PAY');         USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;         call rls_policy_apply_sp('{{ env_var('DBT_CORE_DB') }}','CMP_CORE_SCH','FACT_VARIABLE_PAY');",
    )
}}
with
    detailed_report as (
        select user_id, team_rating, individual_rating, variable_pay_program_name
        from {{ ref("stg_variable_pay_report") }}
        qualify
            row_number() over (
                partition by user_id, variable_pay_program_name order by end_date desc
            )
            = 1
    ),
    cmp_detail as (
        select
            user_id,
            fcyt_one_time_bonus_fcyt,
            fcyt_fk_all_players_key_dalp,
            substr(dcam_lb_label_dcam, 1, 4) as cmp_year
        from {{ ref("fact_comp_yer_transact") }}
        inner join
            {{ ref("dim_campaign") }} on dcam_pk_campaign_dcam = fcyt_fk_campaign_dcam
        qualify
            row_number() over (
                partition by user_id, cmp_year order by campaign_end_date desc
            )
            = 1
    ),
    variable_pay as (
        select
            svpr.user_id,
            svpr.person_id_external,
            empl_pk_employee_empl,
            dpvp_pk_variable_pay_campaign_dpvp,
            to_date(
                dpvp_variable_pay_campaign_year_dpvp || '-12-31'
            ) as dcam_lb_fg_end_date_dcam,
            to_date(
                dpvp_variable_pay_campaign_year_dpvp || '-01-01'
            ) as dcam_lb_fg_start_date_dcam,
            dr.team_rating as tg,
            dr.individual_rating as ig,
            bonusable_salary,
            total_target_amount,
            payout_percent,
            business_unit_payout,
            business_target,
            team_rating,
            business_rating,
            team_result_payout_amount,
            people_target,
            individual_rating,
            people_rating,
            individual_result_payout_amount,
            final_bonus_payout,
            payout_as_percent_of_target,
            payout_as_percent_of_bonusable_salary,
            fcyt_one_time_bonus_fcyt,
            dap.play_pk_play as all_players_key,
            dg.gend_pk_gend as gender_key,
            round(
                iff(
                    business_target <> 0,
                    (team_result_payout_amount / business_target) * 100,
                    0
                ),
                2
            ) as bg_payout_percent,
            round(
                iff(
                    people_target <> 0,
                    (individual_result_payout_amount / people_target) * 100,
                    0
                ),
                2
            ) as pg_payout_percent,
            case
                when
                    dr.team_rating = dpvg.dvpg_rating_dvpg
                    and svpr.business_rating
                    between dpvg.dvpg_min_guidelines_dvpg
                    and dpvg.dvpg_max_guidelines_dvpg
                then 'Aligned'
                when svpr.business_rating is null
                then null
                else 'Not Aligned'
            end as team_rating_guidlines_alined,
            case
                when
                    dr.individual_rating = dpvig.dvpg_rating_dvpg
                    and svpr.people_rating
                    between dpvig.dvpg_min_guidelines_dvpg
                    and dpvig.dvpg_max_guidelines_dvpg
                then 'Aligned'
                when svpr.people_rating is null
                then null
                else 'Not Aligned'
            end as people_rating_guidlines_alined,
            hire_date,
            svpr.variable_pay_program_name,
            dpgkp.guideline_code as people_guidelines_aligned_key,
            dpgkb.guideline_code as business_guidelines_aligned_key,
            case
                when bonusable_salary <> 0
                then round(total_target_amount * 100 / bonusable_salary, 1)
                else 0.0
            end as percent_of_prorated_bonusable_salary,
            svpr.comp_plan_owner,
            dcpv.comp_planner_full_name,
            dcpv.dcpv_comp_plan_sk_ddep
        from {{ ref("stg_variable_pay_global_report") }} svpr
        inner join
            {{ ref("dim_employee") }} de
            on svpr.person_id_external = de.empl_person_id_external_empl
            and svpr.user_id = de.empl_user_id_empl
        left outer join
            {{ ref("dim_param_variable_pay_campaign") }} dpvc
            on dpvc.dpvp_lb_label_dpvp = svpr.variable_pay_program_name
        left outer join
            cmp_detail fcyt
            on fcyt.user_id = svpr.user_id
            and fcyt.cmp_year = substr(svpr.variable_pay_program_name, 1, 4)

        left outer join {{ ref("dim_gender") }} dg on svpr.gender = dg.gend_code_gend
        inner join
            detailed_report dr
            on svpr.user_id = dr.user_id
            and substr(svpr.variable_pay_program_name, 1, 4)
            = substr(dr.variable_pay_program_name, 1, 4)
        inner join
            {{ ref("dim_param_variable_pay_guidelines") }} dpvg
            on dpvg.dvpg_fk_variable_pay_campaign_dpvp
            = dpvc.dpvp_pk_variable_pay_campaign_dpvp
            and dpvg.dvpg_rating_dvpg = dr.team_rating
        inner join
            {{ ref("dim_param_variable_pay_guidelines") }} dpvig
            on dpvig.dvpg_fk_variable_pay_campaign_dpvp
            = dpvc.dpvp_pk_variable_pay_campaign_dpvp
            and dpvig.dvpg_rating_dvpg = dr.individual_rating
        left outer join
            {{ ref("dim_all_players") }} dap
            on svpr.all_players_status = dap.play_label_play
        left outer join
            {{ ref("dim_param_guidelines_key") }} dpgkp
            on dpgkp.people_guidelines_aligned = people_rating_guidlines_alined
        left outer join
            {{ ref("dim_param_guidelines_key") }} dpgkb
            on dpgkb.team_guidelines_aligned = team_rating_guidlines_alined
        left outer join
            {{ ref("dim_compensation_planner") }} dcpv
            on svpr.comp_plan_owner  = dcpv.comp_planner_user_id
    ),
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

    ),
    job_arch as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by user_id
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
                            variable_pay cn
                            on sejf.user_id = cn.user_id
                            and start_date <= dcam_lb_fg_end_date_dcam
                            and iff(end_date = '1978-01-11', '9999-12-31', end_date)
                            >= dcam_lb_fg_start_date_dcam
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
                            and calc_joar_dt_end_joar >= dcam_lb_fg_start_date_dcam
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
    ),
    job_arch_seq as (
        select nvl(job_arch.position, seq.position) as position1, job_arch.*
        from job_arch
        inner join seq on job_arch.user_id = seq.user_id
    ),  -- select * from job_arch_seq; --8,270
    dim_org as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by user_id order by orgn_dt_begin_orgn desc
                    ) as rk_do,
                    *
                from
                    (
                        select distinct
                            user_id,
                            -- position,
                            position1 as position,
                            orgn_pk_organisation_orgn,
                            cust_operationalarea,
                            cust_scopetype,
                            orgn_dt_begin_orgn,
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
                            and calc_orgn_dt_end_orgn >= dcam_lb_fg_start_date_dcam
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
                    -- and effective_start_date < dcam_lb_fg_end_date_dcam
                    -- and calc_effective_end_date > dcam_lb_fg_start_date_dcam
                    )
            )
        where rk_do = 1
    ),  -- select * from dim_org; where user_id='00515614'; --1001259
    dim_org_v1 as (
        select
            organization_sk,
            dov.cost_center_code,
            organization_start_date,
            organization_end_date,
            job_start_date,
            job_end_date,
            dj.user_id,
            cn.dcam_lb_fg_start_date_dcam
        from variable_pay cn
        inner join
            {{ env_var("DBT_PUB_DB") }}.cmn_pub_sch.dim_job dj
            on dj.user_id = cn.user_id
            and job_start_date <= dcam_lb_fg_end_date_dcam
            and job_end_date >= dcam_lb_fg_start_date_dcam
        inner join
            {{ env_var("DBT_PUB_DB") }}.cmn_pub_sch.dim_organization_v1 dov
            on dov.cost_center_code = dj.cost_center_code
            and organization_start_date <= dcam_lb_fg_end_date_dcam
            and organization_end_date >= dcam_lb_fg_start_date_dcam

        qualify
            row_number() over (
                partition by dj.user_id, dcam_lb_fg_start_date_dcam
                order by job_end_date desc, organization_end_date desc
            )
            = 1
    ),  -- SELECT * FROM DIM_ORG_V1;
    team_rating as (

        select *, substr(vp.variable_pay_program_name, 1, 4) as team_rating_year
        from variable_pay vp
        inner join
            {{ ref("dim_param_variable_pay_guidelines") }} dpvg
            on dpvg.dvpg_fk_variable_pay_campaign_dpvp
            = vp.dpvp_pk_variable_pay_campaign_dpvp
            and dpvg.dvpg_rating_dvpg = vp.tg

    ),
    individual_rating as (

        select *, substr(vp.variable_pay_program_name, 1, 4) as individual_rating_year
        from variable_pay vp
        inner join
            {{ ref("dim_param_variable_pay_guidelines") }} dpvg
            on dpvg.dvpg_fk_variable_pay_campaign_dpvp
            = vp.dpvp_pk_variable_pay_campaign_dpvp
            and dpvg.dvpg_rating_dvpg = vp.ig

    ),
    range_payout as

    (select * from {{ ref("dim_range") }} where range_type = 'Payout'),
    range_job as

    (select * from {{ ref("dim_range") }} where range_type = 'Job'),
    range_group as

    (select * from {{ ref("dim_range") }} where range_type = 'Group'),
    range_age as

    (select * from {{ ref("dim_range") }} where range_type = 'Age'),
    group_sen as (

        select distinct
            cn.*,
            seef.user_id as user_id_g,
            custom_date2 as group_seniority_date,
            round(
                months_between(dcam_lb_fg_end_date_dcam, group_seniority_date)
            ) as group_seniority_mth,
            substr(cn.variable_pay_program_name, 1, 4) as group_sen_year
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
            variable_pay cn
            -- on seef.user_id = cn.user_id
            on seef.person_id_external = cn.person_id_external
            and group_seniority_date <= dcam_lb_fg_end_date_dcam
    ),  -- select * from GROUP_SEN;
    group_sen_no_filter as (
        select distinct
            cn.*,
            seef.user_id as user_id_g,
            -- seef.PERSON_ID_EXTERNAL,
            -- custom_date2 as group_seniority_date,
            custom_date2 as group_seniority_date,
            round(
                months_between(dcam_lb_fg_end_date_dcam, group_seniority_date)
            ) as group_seniority_mth_no_filter,
            substr(cn.variable_pay_program_name, 1, 4) as group_sen_year_no_filter
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
            variable_pay cn
            -- on seef.user_id = cn.user_id
            on seef.person_id_external = cn.person_id_external
            and group_seniority_date <= dcam_lb_fg_end_date_dcam

    ),  -- select * from group_sen_no_filter where PERSON_ID_EXTERNAL='70316951';
    age as (
        select
            round(months_between(dcam_lb_fg_end_date_dcam, dob)) as age_mth,
            *,
            substr(cn.variable_pay_program_name, 1, 4) as age_year
        from
            (
                select empl_user_id_empl as user_id_a, empl_date_of_birth_empl as dob

                from {{ ref("dim_employee") }}
            ) dee
        inner join variable_pay cn on dee.user_id_a = cn.user_id

    ),  -- select * from AGE;
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
    va_p.user_id as ftvp_user_id_ftvp,
    va_p.person_id_external as ftvp_person_id_external_ftvp,
    (
        va_p.empl_pk_employee_empl || (to_char(va_p.dcam_lb_fg_end_date_dcam, 'YYYYMM'))
    )::number(38, 0) date_employee_key,
    nvl(va_p.empl_pk_employee_empl, '-1') ftvp_fk_empl_ftvp,
    nvl(va_p.dpvp_pk_variable_pay_campaign_dpvp, '-1') as ftvp_fk_variable_pay_ftvp,
    nvl(jo_a.joar_pk_joar, '-1') as ftvp_fk_job_architecture_key_ftvp,
    nvl(di_o.orgn_pk_organisation_orgn, '-1') as ftvp_fk_organization_ftvp,
    va_p.hire_date as ftvp_hire_date_ftvp,
    nvl(tr.dvpg_pk_guidelines_dvpg, '1') as ftvp_business_rating_guideline_dvpg,
    nvl(ir.dvpg_pk_guidelines_dvpg, '-1') as ftvp_people_rating_guideline_dvpg,
    va_p.bonusable_salary ftvp_bonusable_salary_ftvp,
    va_p.total_target_amount ftvp_target_amount_ftvp,
    va_p.payout_percent ftvp_bu_payout_percent_ftvp,
    va_p.business_unit_payout ftvp_bu_payout_ftvp,
    va_p.business_target ftvp_bg_business_target_ftvp,
    va_p.team_rating ftvp_bg_rating_ftvp,
    va_p.business_rating ftvp_bg_business_rating_ftvp,
    va_p.team_result_payout_amount ftvp_bg_team_result_payout_amount_ftvp,
    va_p.people_target ftvp_pg_people_target_ftvp,
    va_p.individual_rating ftvp_pg_rating_ftvp,
    va_p.people_rating ftvp_pg_people_rating_ftvp,
    va_p.individual_result_payout_amount ftvp_pg_individual_result_payout_amount_ftvp,
    va_p.final_bonus_payout ftvp_total_final_bonus_payout_ftvp,
    va_p.payout_as_percent_of_target ftvp_total_payout_as_percent_of_target_ftvp,
    va_p.payout_as_percent_of_bonusable_salary ftvp_total_payout_as_percent_of_bonusable_salary_ftvp,
    va_p.bg_payout_percent as ftvp_bg_payout_percent_ftvp,
    va_p.pg_payout_percent as ftvp_pg_payout_percent_ftvp,
    cast(
        va_p.fcyt_one_time_bonus_fcyt as number(38, 2)
    ) as ftvp_fcyt_one_time_bonus_fcyt,
    nvl(va_p.all_players_key, '-1') as ftvp_fk_all_players_key_fcyt,
    nvl(rj.range_code, -1) as ftvp_job_seniority_range_code_ftvp,
    nvl(
        ifnull(rg.range_code, rg_no_filter.range_code), -1
    ) as ftvp_group_seniority_range_code_ftvp,
    nvl(ra.range_code, -1) as ftvp_age_range_code_ftvp,
    nvl(rp.range_code, -1) as ftvp_payout_range_code_ftvp,
    va_p.team_rating_guidlines_alined as ftvp_team_rating_guidlines_aligned_ftvp,
    va_p.people_rating_guidlines_alined as ftvp_people_rating_guidlines_aligned_ftvp,
    nvl(va_p.gender_key, '-1') as fcyt_pk_gender_fcyt,
    nvl(dlpg.lpgr_pk_lpgr, '-1') as ftvp_pk_local_pay_grade_lpgr,
    nvl(dgpg.gpgr_pk_gpgr, '-1') as ftvp_pk_global_pay_grade_gpgr,
    nvl(deg.emgp_pk_emgp, '-1') as ftvp_pk_employee_group_emgp,
    nvl(des.emsg_pk_emsg, '-1') as ftvp_pk_employee_subgroup_emsg,
    nvl(dhh.hoho_pk_hoho, '-1') as ftvp_pk_home_host_hoho,
    nvl(doa.opar_pk_opar, '-1') as ftvp_pk_operational_area_opar,
    nvl(dst.scot_pk_scot, '-1') as ftvp_pk_scope_type_scot,
    nvl(dct.coty_pk_coty, '-1') as ftvp_pk_contract_type_coty,
    nvl(emp_status, '-1') as ftvp_empl_status_ftvp,
    nvl(dejl.jlvl_pk_jlvl, '-1') as ftvp_pk_job_level_jlvl,
    nvl(sk_ethnicity_id, '-1') as ftvp_pk_ethnicity_ecty,
    nvl(empm_pk_hr_id_empm, '-1') as ftvp_sk_hr_id_empm,
    nvl(dkp.kpos_pk_kpos, '-1') as ftvp_pk_kpos,
    va_p.people_guidelines_aligned_key as ftvp_people_guidelines_aligned_key,
    va_p.business_guidelines_aligned_key as ftvp_business_guidelines_aligned_key,
    va_p.percent_of_prorated_bonusable_salary
    as ftvp_percent_of_prorated_bonusable_salary,
    nvl(dov1.organization_sk, '-1') as ftvp_fk_organization_v1_sk_ftvp,
    va_p.comp_planner_full_name as ftvp_compensation_planner_empl,
    va_p.comp_plan_owner as ftvp_comp_plan_owner_ftvp,
    nvl(va_p.dcpv_comp_plan_sk_ddep, '-1') as ftvp_comp_plan_sk_dcpv
from variable_pay va_p
left outer join job_arch jo_a on va_p.user_id = jo_a.user_id
left outer join dim_org di_o on va_p.user_id = di_o.user_id
left outer join
    team_rating tr
    on va_p.user_id = tr.user_id
    and va_p.person_id_external = tr.person_id_external
    and tr.team_rating_year = substr(va_p.variable_pay_program_name, 1, 4)
left outer join
    individual_rating ir
    on va_p.user_id = ir.user_id
    and va_p.person_id_external = ir.person_id_external
    and ir.individual_rating_year = substr(va_p.variable_pay_program_name, 1, 4)
left outer join
    group_sen
    on va_p.user_id = group_sen.user_id
    and group_sen_year = substr(va_p.variable_pay_program_name, 1, 4)
left outer join
    group_sen_no_filter
    on va_p.user_id = group_sen_no_filter.user_id
    and group_sen_year_no_filter = substr(va_p.variable_pay_program_name, 1, 4)
left outer join
    age
    on va_p.user_id = age.user_id_a
    and age_year = substr(va_p.variable_pay_program_name, 1, 4)
left outer join range_job rj on job_seniority_mth between rj.range_inf and rj.range_sup
left outer join
    range_group rg on group_seniority_mth between rg.range_inf and rg.range_sup
left outer join
    range_group rg_no_filter
    on group_seniority_mth_no_filter
    between rg_no_filter.range_inf and rg_no_filter.range_sup
left outer join range_age ra on age_mth between ra.range_inf and ra.range_sup
left outer join
    range_payout rp
    on va_p.payout_as_percent_of_target > rp.range_inf
    and va_p.payout_as_percent_of_target <= rp.range_sup
left outer join
    {{ ref("dim_local_pay_grade") }} dlpg
    on (jo_a.country_of_company || jo_a.pay_grade) = dlpg.lpgr_code_lpgr
left outer join
    {{ ref("dim_global_pay_grade") }} dgpg on jo_a.pay_grade = dgpg.gpgr_code_gpgr
left outer join
    {{ ref("dim_employee_group") }} deg on jo_a.custom_string4 = deg.emgp_code_emgp
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
    on jo_a.custom_string31 = des.emsg_code_emsg
left outer join
    {{ ref("dim_home_host") }} dhh on jo_a.custom_string12 = dhh.hoho_id_hoho
left outer join
    {{ ref("dim_operational_area") }} doa
    on di_o.cust_operationalarea = doa.opar_code_opar
left outer join
    {{ ref("dim_scope_type") }} dst on di_o.cust_scopetype = dst.scot_code_scot
left outer join
    {{ ref("dim_contract_type") }} dct on jo_a.contract_type = dct.coty_id_coty
left outer join {{ ref("dim_job_level") }} dejl on di_o.job_level = dejl.jlvl_code_jlvl
left outer join ethnicity on va_p.person_id_external = ethnicity.personidexternal
left outer join hr_manager hm on hm.empm_pk_user_id_empl = va_p.empl_pk_employee_empl
left outer join
    {{ ref("dim_key_position") }} dkp
    on di_o.cust_key_position_level = dkp.kpos_code_kpos
left outer join
    dim_org_v1 dov1
    on va_p.user_id = dov1.user_id
    and va_p.dcam_lb_fg_start_date_dcam = dov1.dcam_lb_fg_start_date_dcam
