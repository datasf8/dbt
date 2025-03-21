{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','HEADCOUNT');"
    )
}}

with
    country_csrd as (
        select distinct country_code from {{ ref("csrd_employee_group_mapping") }}
    ),

    empstatus as (

        select *
        from {{ ref("employee_status_v1") }}
        qualify
            row_number() over (
                partition by employee_status_code
                order by employee_status_start_date desc
            )
            = 1
    ),
    empgroup as (
        select *
        from {{ ref("employee_group_v1") }}
        qualify
            row_number() over (
                partition by employee_group_code order by employee_group_start_date desc
            )
            = 1
    ),
    yesno as (
        select *
        from {{ ref("yes_no_flag_referential_v1") }}
        qualify
            row_number() over (
                partition by yes_no_flag_referential_code
                order by yes_no_flag_referential_start_date desc
            )
            = 1
    ),
    jobinfo as (

        select jinfo.*, country_code
        from {{ ref("job_information_v1") }} jinfo
        left outer join
            {{ ref("cost_center_v1") }} cc
            on jinfo.cost_center_code = cc.cost_center_code
            and jinfo.job_start_date
            between cc.cost_center_start_date and cc.cost_center_end_date

        left outer join
            {{ ref("business_unit_v1") }} bu
            on bu.business_unit_code = cc.business_unit_code
            and cc.cost_center_start_date
            between bu.business_unit_start_date and bu.business_unit_end_date

        left outer join
            {{ ref("company_v1") }} comp
            on comp.company_code = bu.company_code
            and bu.business_unit_start_date
            between comp.company_start_date and comp.company_end_date
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    job as (
        select
            ji.user_id,
            ji.job_start_date,
            ji.job_end_date,
            iff(
                (
                    (
                        es.employee_status_name_en in ('Active', 'Paid Leave')
                        and nvl(yn.yes_no_flag_referential_code, 'Y') = 'Y'
                    )
                    or (
                        es.employee_status_name_en = 'Unpaid Leave'
                        and yn.yes_no_flag_referential_code = 'Y'
                    )
                )
                and eg.employee_group_name_en
                in ('Expatriate (host)', 'Long Term Employee', 'Fixed Term Employee')
                and (
                    datediff(
                        'dd',
                        company_entry_date,
                        nvl(contract_end_date, '9999-12-31')
                    ) >= iff(
                        eg.employee_group_name_en = 'Fixed Term Employee'
                        and sp.specialization_name_en
                        in ('E-Beauty Advisory', 'Beauty Advisory'),
                        30,
                        -999999
                    )
                ),
                1,
                0
            ) sta,
            iff(
                es.employee_status_name_en in ('Active', 'Paid Leave', 'Unpaid Leave')
                and eg.employee_group_name_en in (
                    'Apprenticeship',
                    'Expatriate (host)',
                    'Fixed Term Employee',
                    'Long Term Employee'
                ),
                1,
                0
            ) rel,
            iff(
                employee_status_name_en in ('Active', 'Paid Leave', 'Unpaid Leave')
                and eg.employee_group_name_en in (
                    'Apprenticeship',
                    'Expatriate (host)',
                    'Fixed Term Employee',
                    'Internship',
                    'Long Term Employee'
                ),
                1,
                0
            ) "ALL",
            iff(
                employee_status_name_en in ('Active', 'Paid Leave', 'Unpaid Leave')
                and mapping.country_code is not null,
                iff(mapping_eg.employee_group_code is not null, 1, 0),
                iff(
                    employee_status_name_en in ('Active', 'Paid Leave', 'Unpaid Leave')
                    and eg.employee_group_name_en in (
                        'Apprenticeship',
                        'Expatriate (host)',
                        'Fixed Term Employee',
                        'Internship',
                        'Long Term Employee'
                    ),
                    1,
                    0
                )
            ) as csr
        from jobinfo ji
        left join empstatus es on ji.employee_status_id = es.employee_status_id
        left join empgroup eg on ji.employee_group_code = eg.employee_group_code
        left join
            yesno yn on ji.included_headcount_yn_flag_id = yn.yes_no_flag_referential_id
        left join
            {{ ref("position_v1") }} pos
            on ji.position_code = pos.position_code
            and ji.job_start_date
            between pos.position_start_date and pos.position_end_date
        left join
            {{ ref("job_role_v1") }} jr
            on pos.job_role_code = jr.job_role_code
            and ji.job_start_date between job_role_start_date and job_role_end_date
        left join
            {{ ref("specialization_v1") }} sp
            on jr.specialization_code = sp.specialization_code
            and ji.job_start_date
            between specialization_start_date and specialization_end_date

        left outer join country_csrd mapping on ji.country_code = mapping.country_code
        left outer join
            {{ ref("csrd_employee_group_mapping") }} mapping_eg
            on ji.country_code = mapping_eg.country_code
            and ji.employee_group_code = mapping_eg.employee_group_code
    ),  -- select * from job;
    final_hc as (
        select *
        from
            job unpivot (
                headcount_present_flag for headcount_type_code in (sta, rel, "ALL", csr)
            )
        order by 4, 1, 2
    ),
    surrogate_key as (
        select
            p.*, hash(user_id, job_start_date, headcount_type_code) as headcount_line_id
        from final_hc p
    )
select
    headcount_line_id,
    user_id,
    job_start_date,
    job_end_date,
    headcount_type_code,
    headcount_present_flag
from surrogate_key
join {{ ref("employee_profile_directory") }} using (user_id)
