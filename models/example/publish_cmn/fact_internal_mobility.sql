{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change='sync_all_columns',
        cluster_by=['user_id','mobility_date','headcount_type_code'],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_INTERNAL_MOBILITY');"
    )
}}
with
    emp_details as (
        select
            *,
            nvl(
                group_seniority,
                lag(group_seniority) ignore nulls over (
                    partition by personal_id order by employment_details_start_date
                )
            ) as last_group_seniority
        from {{ ref("employment_details_v1") }}
    ),
    job_info_raw as (
        select ji.*, personal_id
        from {{ ref("job_information_v1") }} ji
        left join
            (
                select distinct
                    user_id,
                    personal_id,
                    employment_details_start_date,
                    employment_details_end_date
                from emp_details
            ) ed
            on ji.user_id = ed.user_id
            and ji.job_start_date
            between ed.employment_details_start_date and ed.employment_details_end_date
        -- added date condition to fill personal_id null for terminated user
        qualify
            row_number() over (
                partition by ji.user_id, ji.job_start_date
                order by ji.sequence_number desc
            )
            = 1
    ),
    job_info as (
        select
            *,
            lag(iff(employee_group_code = 'EG0005', user_id, null)) ignore nulls over (  -- Expatriate (home)
                partition by personal_id order by job_start_date, employee_group_code
            ) home_user_id
        from job_info_raw
    ),
    hc as (
        select headcount_type_code, user_id, job_start_date, headcount_present_flag
        from {{ ref("headcount_v1") }}
        where headcount_present_flag = 1
    ),
    ji_pos as (
        select *
        from job_info
        qualify
            row_number() over (
                partition by position_code, job_start_date order by job_end_date desc
            )
            = 1
        order by job_start_date desc
    ),
    hr as (
        select
            j1.user_id,
            j1.job_start_date,
            j1.position_code,
            pl.related_position,
            j2.user_id user_id_hr
        from job_info j1
        left join
            {{ ref("position_relationship_v1") }} pl
            on j1.position_code = pl.position_code
            and j1.job_start_date
            between pl.position_relationship_start_date
            and pl.position_relationship_end_date
        left join
            ji_pos j2
            on pl.related_position = j2.position_code
            and j1.job_start_date between j2.job_start_date and j2.job_end_date
        qualify
            row_number() over (
                partition by j1.user_id, j1.job_start_date order by j1.job_end_date desc
            )
            = 1
    ),
    dim_position as (
        select
            position_sk,
            position_code,
            position_start_date,
            position_end_date,
            location_code,
            location_name as location
        from {{ ref("dim_position_v1") }}
    ),
    dim_organization as (
        select
            organization_sk,
            cost_center_code,
            organization_start_date,
            organization_end_date,
            hr_division_code,
            company_code,
            business_unit_code,
            country_code,
            business_unit_type_code,
            is_ec_live_flag,
            hr_division_name_en as division,
            company_name_en as company,
            business_unit_name_en as business_unit,
            country_name_en as country,
            business_unit_type_name_en as business_unit_type
        from {{ ref("dim_organization_v1") }}
    ),
    dim_job as (
        select
            job_sk,
            user_id,
            job_start_date,
            employee_group_code,
            functional_area_code,
            organizational_area_code,
            global_grade_code,
            job_role_code,
            specialization_code,
            professional_field_code,
            brand_code,
            employee_group_name_en as employee_group,
            functional_area_name_en as functional_area,
            organizational_area_name_en as organizational_area,
            global_grade_name as global_grade,
            job_role_name_en as job_role,
            specialization_name_en as specialization,
            professional_field_name_en as professional_field,
            brand_name_en as brand
        from {{ ref("dim_job") }}
    ),
    job_mobility as (
        select
            ji.user_id,
            ji.job_start_date,
            ji.job_end_date,
            nvl(job.job_sk, -1) as job_sk,
            ji.job_start_date as effective_date,
            nvl(org.organization_sk, -1) as organization_sk,
            nvl(pos.position_sk, -1) as position_sk,
            lag(ji.job_start_date) over (
                partition by ji.user_id order by ji.job_start_date
            ) prev_job_start_date,
            lag(job_sk) over (
                partition by ji.user_id order by effective_date
            ) as from_job_sk,
            lag(organization_sk) over (
                partition by ji.user_id order by effective_date
            ) as from_organization_sk,
            lag(position_sk) over (
                partition by ji.user_id order by effective_date
            ) as from_position_sk
        from job_info ji
        inner join
            dim_job job
            on ji.user_id = job.user_id
            and ji.job_start_date = job.job_start_date
        left join
            dim_position pos
            on ji.position_code = pos.position_code
            and ji.job_start_date between position_start_date and position_end_date
        inner join
            dim_organization org
            on ji.cost_center_code = org.cost_center_code
            and ji.job_start_date
            between organization_start_date and organization_end_date
            and org.is_ec_live_flag = true
    ),
    internal_mobility as (
        select
            nvl(im.job_sk, -1) as job_mobility_sk,
            im.user_id,
            im.effective_date,
            im.job_start_date,
            im.job_end_date,
            im.from_job_sk,
            im.job_sk as job_sk,
            im.from_organization_sk,
            im.organization_sk as organization_sk,
            im.from_position_sk,
            im.position_sk as position_sk,
            op.division as from_division,
            oc.division as division,
            case
                when nvl(oc.hr_division_code, '-1') <> nvl(op.hr_division_code, '-1')
                then 1
                else 0
            end division_flag,
            op.company as from_company,
            oc.company as company,
            case
                when nvl(oc.company_code, '-1') <> nvl(op.company_code, '-1')
                then 1
                else 0
            end company_flag,
            op.business_unit as from_business_unit,
            oc.business_unit as business_unit,
            case
                when
                    nvl(oc.business_unit_code, '-1') <> nvl(op.business_unit_code, '-1')
                then 1
                else 0
            end business_unit_flag,
            op.country as from_country,
            oc.country as country,
            case
                when nvl(oc.country_code, '-1') <> nvl(op.country_code, '-1')
                then 1
                else 0
            end country_flag,
            op.business_unit_type as from_business_unit_type,
            oc.business_unit_type as business_unit_type,
            case
                when
                    nvl(oc.business_unit_type_code, '-1')
                    <> nvl(op.business_unit_type_code, '-1')
                then 1
                else 0
            end business_unit_type_flag,
            jp.employee_group as from_employee_group,
            jc.employee_group as employee_group,
            case
                when
                    nvl(jc.employee_group_code, '-1')
                    <> nvl(jp.employee_group_code, '-1')
                then 1
                else 0
            end employee_group_flag,
            jp.functional_area as from_functional_area,
            jc.functional_area as functional_area,
            case
                when
                    nvl(jc.functional_area_code, '-1')
                    <> nvl(jp.functional_area_code, '-1')
                then 1
                else 0
            end functional_area_flag,
            jp.organizational_area as from_organizational_area,
            jc.organizational_area as organizational_area,
            case
                when
                    nvl(jc.organizational_area_code, '-1')
                    <> nvl(jp.organizational_area_code, '-1')
                then 1
                else 0
            end organizational_area_flag,
            jp.global_grade as from_global_grade,
            jc.global_grade as global_grade,
            case
                when nvl(jc.global_grade_code, '-1') <> nvl(jp.global_grade_code, '-1')
                then 1
                else 0
            end global_grade_flag,
            jp.job_role as from_job_role,
            jc.job_role as job_role,
            case
                when nvl(jc.job_role_code, '-1') <> nvl(jp.job_role_code, '-1')
                then 1
                else 0
            end job_role_flag,
            jp.specialization as from_specialization,
            jc.specialization as specialization,
            case
                when
                    nvl(jc.specialization_code, '-1')
                    <> nvl(jp.specialization_code, '-1')
                then 1
                else 0
            end specialization_flag,
            jp.professional_field as from_professional_field,
            jc.professional_field as professional_field,
            case
                when
                    nvl(jc.professional_field_code, '-1')
                    <> nvl(jp.professional_field_code, '-1')
                then 1
                else 0
            end professional_field_flag,
            pp.location as from_location,
            pc.location as location,
            case
                when nvl(pc.location_code, '-1') <> nvl(pp.location_code, '-1')
                then 1
                else 0
            end location_flag,
            jp.brand as from_brand,
            jc.brand as brand,
            case
                when nvl(jc.brand_code, '-1') <> nvl(jp.brand_code, '-1') then 1 else 0
            end brand_flag,
            case
                when
                    1 like any (
                        division_flag,
                        job_role_flag,
                        company_flag,
                        business_unit_flag,
                        country_flag,
                        business_unit_type_flag,
                        employee_group_flag,
                        functional_area_flag,
                        organizational_area_flag,
                        global_grade_flag,
                        specialization_flag,
                        professional_field_flag,
                        location_flag,
                        brand_flag
                    )
                then 1
                else 0
            end as job_mobility_flag
        from job_mobility im
        left join dim_organization oc on im.organization_sk = oc.organization_sk
        left join dim_organization op on im.from_organization_sk = op.organization_sk
        left join dim_position pc on im.position_sk = pc.position_sk
        left join dim_position pp on im.from_position_sk = pp.position_sk
        left join dim_job jc on im.job_sk = jc.job_sk
        left join dim_job jp on im.from_job_sk = jp.job_sk
        where prev_job_start_date is not null and job_mobility_flag = 1
    ),
    internal_mobility_v1 as (
        select
            user_id,
            effective_date,
            'From' type,
            job_sk as job_sk,
            from_job_sk as job_sk_fltr,
            organization_sk as organization_sk,
            position_sk as position_sk,
            from_division as division,
            from_company as company,
            from_business_unit as business_unit,
            from_country as country,
            from_business_unit_type as business_unit_type,
            from_employee_group as employee_group,
            from_functional_area as functional_area,
            from_organizational_area as organizational_area,
            from_global_grade as global_grade,
            from_job_role as job_role,
            from_specialization as specialization,
            from_professional_field as professional_field,
            from_location as location,
            from_brand as brand,
            division_flag,
            job_role_flag,
            company_flag,
            business_unit_flag,
            country_flag,
            business_unit_type_flag,
            employee_group_flag,
            functional_area_flag,
            organizational_area_flag,
            global_grade_flag,
            specialization_flag,
            professional_field_flag,
            location_flag,
            brand_flag
        from internal_mobility
        union all
        select
            user_id,
            effective_date,
            'To' type,
            job_sk,
            job_sk as job_sk_fltr,
            organization_sk,
            position_sk,
            from_division,
            from_company,
            from_business_unit,
            from_country,
            from_business_unit_type,
            from_employee_group,
            from_functional_area,
            from_organizational_area,
            from_global_grade,
            from_job_role,
            from_specialization,
            from_professional_field,
            from_location,
            from_brand,
            division_flag,
            job_role_flag,
            company_flag,
            business_unit_flag,
            country_flag,
            business_unit_type_flag,
            employee_group_flag,
            functional_area_flag,
            organizational_area_flag,
            global_grade_flag,
            specialization_flag,
            professional_field_flag,
            location_flag,
            brand_flag
        from internal_mobility
    ),
    final as (
        select
            ji.user_id,
            ji.job_start_date,
            emp.personal_id,
            hr.user_id_hr,
            ji.company_entry_date,
            ji.job_entry_date,
            iff(
                employee_group_code = 'EG0008',  -- Expatriate (host)
                nvl(emp.last_group_seniority, emph.last_group_seniority),
                emp.last_group_seniority
            ) group_seniority_date,
            bi.date_of_birth,
            round(
                months_between(ji.job_start_date, ji.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(ji.job_start_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(ji.job_start_date, bi.date_of_birth), 3
            ) age_seniority_months
        from job_info ji
        left join {{ ref("biographical_information_v1") }} bi using (personal_id)
        left join
            emp_details emp
            on ji.user_id = emp.user_id
            and ji.job_start_date
            between emp.employment_details_start_date
            and emp.employment_details_end_date
        left join
            emp_details emph
            on ji.user_id = emph.user_id
            and ji.job_start_date
            between emph.employment_details_start_date
            and emph.employment_details_end_date
        left join
            hr on ji.user_id = hr.user_id and ji.job_start_date = hr.job_start_date
    )
select
    imv.job_sk as job_mobility_sk,
    ji.user_id,
    ji.personal_id,
    ji.job_start_date as mobility_date,
    to_char(ji.job_start_date, 'YYYYMMDD')::integer as mobility_date_sk,
    to_char(ji.job_start_date, 'YYYYMM')::integer as mobility_month_sk,
    nvl(emp.employee_sk, -1) as employee_sk,
    nvl(emgr.month_employee_sk, -1) as month_employee_sk,
    nvl(hr.employee_sk, -1) as employee_hr_sk,
    ji.user_id_hr,
    imv.type,
    djmi.mobility_sk,
    imv.job_sk,
    imv.organization_sk,
    imv.position_sk,
    hc.headcount_type_code,
    hc.headcount_present_flag,
    ji.company_entry_date,
    ji.job_entry_date,
    ji.group_seniority_date,
    ji.date_of_birth,
    ji.job_seniority_months,
    ji.group_seniority_months,
    ji.age_seniority_months,
    nvl(jsr.range_sk, '-1') as job_seniority_range_sk,
    nvl(gsr.range_sk, '-1') as group_seniority_range_sk,
    nvl(asr.range_sk, '-1') as age_seniority_range_sk,
    jsr.range_name as job_seniority_range_name,
    gsr.range_name as group_seniority_range_name,
    asr.range_name as age_seniority_range_name,
    imv.division,
    imv.company,
    imv.business_unit,
    imv.country,
    imv.business_unit_type,
    imv.employee_group,
    imv.functional_area,
    imv.organizational_area,
    imv.global_grade,
    imv.job_role,
    imv.specialization,
    imv.professional_field,
    imv.location,
    imv.brand,
    imv.division_flag,
    imv.job_role_flag,
    imv.company_flag,
    imv.business_unit_flag,
    imv.country_flag,
    imv.business_unit_type_flag,
    imv.employee_group_flag,
    imv.functional_area_flag,
    imv.organizational_area_flag,
    imv.global_grade_flag,
    imv.specialization_flag,
    imv.professional_field_flag,
    imv.location_flag,
    imv.brand_flag
from final ji
inner join
    internal_mobility_v1 imv
    on ji.user_id = imv.user_id
    and ji.job_start_date = imv.effective_date
inner join
    {{ ref("dim_job_mobility_int") }} djmi
    on imv.job_sk_fltr = djmi.job_sk
    -- and ji.user_id = djmi.user_id
    -- and ji.job_start_date = djmi.effective_date
    and imv.type = djmi.type
left join
    {{ ref("dim_employee_v1") }} emp
    on ji.user_id = emp.user_id
    and ji.job_start_date between emp.employee_start_date and emp.employee_end_date
inner join hc on ji.user_id = hc.user_id and ji.job_start_date = hc.job_start_date
left join
    {{ ref("dim_employee_manager_v1") }} emgr
    on ji.user_id = emgr.user_id
    and mobility_month_sk = emgr.month_sk
    and manager_level = 1
left join
    {{ ref("dim_employee_v1") }} hr
    on ji.user_id_hr = hr.user_id
    and ji.job_start_date between hr.employee_start_date and hr.employee_end_date
left join
    {{ ref("dim_range_v1") }} jsr
    on jsr.range_type = 'JOBSEN'
    and job_seniority_months >= range_start
    and job_seniority_months < range_end
left join
    {{ ref("dim_range_v1") }} gsr
    on gsr.range_type = 'GRPSEN'
    and group_seniority_months >= gsr.range_start
    and group_seniority_months < gsr.range_end
left join
    {{ ref("dim_range_v1") }} asr
    on asr.range_type = 'AGESEN'
    and age_seniority_months / 12 >= asr.range_start
    and age_seniority_months / 12 < asr.range_end
