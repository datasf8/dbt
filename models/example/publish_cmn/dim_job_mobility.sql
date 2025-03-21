{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        cluster_by=["job_sk"],
    )
}}
with
    job_info as (
        select *
        from {{ ref("job_information_v1") }}
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
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
            ) as previous_job_sk,
            lag(organization_sk) over (
                partition by ji.user_id order by effective_date
            ) as previous_organization_sk,
            lag(position_sk) over (
                partition by ji.user_id order by effective_date
            ) as previous_position_sk
        from job_info ji
        left join
            dim_job job
            on ji.user_id = job.user_id
            and ji.job_start_date = job.job_start_date
        left join
            dim_position pos
            on ji.position_code = pos.position_code
            and ji.job_start_date between position_start_date and position_end_date
        left join
            dim_organization org
            on ji.cost_center_code = org.cost_center_code
            and ji.job_start_date
            between organization_start_date and organization_end_date
            and org.is_ec_live_flag = true
    ),
    mobility_data as (
        select
            -- nvl(im.job_sk, -1) as job_mobility_sk,
            im.user_id,
            im.effective_date,
            im.job_start_date,
            im.job_end_date,
            im.job_sk,
            im.previous_job_sk,
            op.division as previous_division,
            oc.division as division,
            case
                when nvl(oc.hr_division_code, '-1') <> nvl(op.hr_division_code, '-1')
                then 1
                else 0
            end division_flag,
            op.company as previous_company,
            oc.company as company,
            case
                when nvl(oc.company_code, '-1') <> nvl(op.company_code, '-1')
                then 1
                else 0
            end company_flag,
            op.business_unit as previous_business_unit,
            oc.business_unit as business_unit,
            case
                when
                    nvl(oc.business_unit_code, '-1') <> nvl(op.business_unit_code, '-1')
                then 1
                else 0
            end business_unit_flag,
            op.country as previous_country,
            oc.country as country,
            case
                when nvl(oc.country_code, '-1') <> nvl(op.country_code, '-1')
                then 1
                else 0
            end country_flag,
            op.business_unit_type as previous_business_unit_type,
            oc.business_unit_type as business_unit_type,
            case
                when
                    nvl(oc.business_unit_type_code, '-1')
                    <> nvl(op.business_unit_type_code, '-1')
                then 1
                else 0
            end business_unit_type_flag,
            jp.employee_group as previous_employee_group,
            jc.employee_group as employee_group,
            case
                when
                    nvl(jc.employee_group_code, '-1')
                    <> nvl(jp.employee_group_code, '-1')
                then 1
                else 0
            end employee_group_flag,
            jp.functional_area as previous_functional_area,
            jc.functional_area as functional_area,
            case
                when
                    nvl(jc.functional_area_code, '-1')
                    <> nvl(jp.functional_area_code, '-1')
                then 1
                else 0
            end functional_area_flag,
            jp.organizational_area as previous_organizational_area,
            jc.organizational_area as organizational_area,
            case
                when
                    nvl(jc.organizational_area_code, '-1')
                    <> nvl(jp.organizational_area_code, '-1')
                then 1
                else 0
            end organizational_area_flag,
            jp.global_grade as previous_global_grade,
            jc.global_grade as global_grade,
            case
                when nvl(jc.global_grade_code, '-1') <> nvl(jp.global_grade_code, '-1')
                then 1
                else 0
            end global_grade_flag,
            jp.job_role as previous_job_role,
            jc.job_role as job_role,
            case
                when nvl(jc.job_role_code, '-1') <> nvl(jp.job_role_code, '-1')
                then 1
                else 0
            end job_role_flag,
            jp.specialization as previous_specialization,
            jc.specialization as specialization,
            case
                when
                    nvl(jc.specialization_code, '-1')
                    <> nvl(jp.specialization_code, '-1')
                then 1
                else 0
            end specialization_flag,
            jp.professional_field as previous_professional_field,
            jc.professional_field as professional_field,
            case
                when
                    nvl(jc.professional_field_code, '-1')
                    <> nvl(jp.professional_field_code, '-1')
                then 1
                else 0
            end professional_field_flag,
            pp.location as previous_location,
            pc.location as location,
            case
                when nvl(pc.location_code, '-1') <> nvl(pp.location_code, '-1')
                then 1
                else 0
            end location_flag,
            jp.brand as previous_brand,
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
        left join
            dim_organization op on im.previous_organization_sk = op.organization_sk
        left join dim_position pc on im.position_sk = pc.position_sk
        left join dim_position pp on im.previous_position_sk = pp.position_sk
        left join dim_job jc on im.job_sk = jc.job_sk
        left join dim_job jp on im.previous_job_sk = jp.job_sk
        where prev_job_start_date is not null and job_mobility_flag = 1
    ),
    data1 as (
        select
            job_sk as job_sk,
            user_id,
            'To' type,
            division,
            company,
            business_unit,
            country,
            business_unit_type,
            employee_group,
            functional_area,
            organizational_area,
            global_grade,
            job_role,
            specialization,
            professional_field,
            location,
            brand,
            previous_division,
            previous_company,
            previous_business_unit,
            previous_country,
            previous_business_unit_type,
            previous_employee_group,
            previous_functional_area,
            previous_organizational_area,
            previous_global_grade,
            previous_job_role,
            previous_specialization,
            previous_professional_field,
            previous_location,
            previous_brand,
            division_flag,
            company_flag,
            business_unit_flag,
            country_flag,
            business_unit_type_flag,
            employee_group_flag,
            functional_area_flag,
            organizational_area_flag,
            global_grade_flag,
            job_role_flag,
            specialization_flag,
            professional_field_flag,
            location_flag,
            brand_flag
        from mobility_data
        where job_mobility_flag = 1
        union all
        select
            previous_job_sk as job_sk,
            user_id,
            'From' type,
            previous_division,
            previous_company,
            previous_business_unit,
            previous_country,
            previous_business_unit_type,
            previous_employee_group,
            previous_functional_area,
            previous_organizational_area,
            previous_global_grade,
            previous_job_role,
            previous_specialization,
            previous_professional_field,
            previous_location,
            previous_brand,
            division,
            company,
            business_unit,
            country,
            business_unit_type,
            employee_group,
            functional_area,
            organizational_area,
            global_grade,
            job_role,
            specialization,
            professional_field,
            location,
            brand,
            division_flag,
            company_flag,
            business_unit_flag,
            country_flag,
            business_unit_type_flag,
            employee_group_flag,
            functional_area_flag,
            organizational_area_flag,
            global_grade_flag,
            job_role_flag,
            specialization_flag,
            professional_field_flag,
            location_flag,
            brand_flag
        from mobility_data
        where job_mobility_flag = 1
    ),
    data2 as (
        select
            *,
            case
                when mobility_type = 'DIVISION'
                then division_flag
                when mobility_type = 'COMPANY'
                then company_flag
                when mobility_type = 'BUSINESS_UNIT'
                then business_unit_flag
                when mobility_type = 'COUNTRY'
                then country_flag
                when mobility_type = 'BUSINESS_UNIT_TYPE'
                then business_unit_type_flag
                when mobility_type = 'EMPLOYEE_GROUP'
                then employee_group_flag
                when mobility_type = 'FUNCTIONAL_AREA'
                then functional_area_flag
                when mobility_type = 'ORGANIZATIONAL_AREA'
                then organizational_area_flag
                when mobility_type = 'GLOBAL_GRADE'
                then global_grade_flag
                when mobility_type = 'JOB_ROLE'
                then job_role_flag
                when mobility_type = 'SPECIALIZATION'
                then specialization_flag
                when mobility_type = 'PROFESSIONAL_FIELD'
                then professional_field_flag
                when mobility_type = 'LOCATION'
                then location_flag
                when mobility_type = 'BRAND'
                then brand_flag
                else null
            end as mobility_type_flag
        from
            data1 unpivot (
                mobility_subtype for mobility_type in (
                    division,
                    company,
                    business_unit,
                    country,
                    business_unit_type,
                    employee_group,
                    functional_area,
                    organizational_area,
                    global_grade,
                    job_role,
                    specialization,
                    professional_field,
                    location,
                    brand
                )
            )
            unpivot (
                mobility_from for mobility_type1 in (
                    previous_division,
                    previous_company,
                    previous_business_unit,
                    previous_country,
                    previous_business_unit_type,
                    previous_employee_group,
                    previous_functional_area,
                    previous_organizational_area,
                    previous_global_grade,
                    previous_job_role,
                    previous_specialization,
                    previous_professional_field,
                    previous_location,
                    previous_brand
                )
            )
        where 'PREVIOUS_' || mobility_type = mobility_type1
    )
select
    djmi.user_id,
    d.job_sk,
    mobility_sk,
    d.type,
    initcap(replace(mobility_type, '_', ' ')) as mobility_type,
    mobility_subtype,
    mobility_from,
    mobility_type_flag
from data2 d
join
    {{ ref("dim_job_mobility_int") }} djmi
    on d.job_sk = djmi.job_sk
    and d.type = djmi.type
