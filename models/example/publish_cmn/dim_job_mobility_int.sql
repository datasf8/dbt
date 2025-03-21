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
            case
                when nvl(oc.hr_division_code, '-1') <> nvl(op.hr_division_code, '-1')
                then 1
                else 0
            end division_flag,
            case
                when nvl(oc.company_code, '-1') <> nvl(op.company_code, '-1')
                then 1
                else 0
            end company_flag,
            case
                when
                    nvl(oc.business_unit_code, '-1') <> nvl(op.business_unit_code, '-1')
                then 1
                else 0
            end business_unit_flag,
            case
                when nvl(oc.country_code, '-1') <> nvl(op.country_code, '-1')
                then 1
                else 0
            end country_flag,
            case
                when
                    nvl(oc.business_unit_type_code, '-1')
                    <> nvl(op.business_unit_type_code, '-1')
                then 1
                else 0
            end business_unit_type_flag,
            case
                when
                    nvl(jc.employee_group_code, '-1')
                    <> nvl(jp.employee_group_code, '-1')
                then 1
                else 0
            end employee_group_flag,
            case
                when
                    nvl(jc.functional_area_code, '-1')
                    <> nvl(jp.functional_area_code, '-1')
                then 1
                else 0
            end functional_area_flag,
            case
                when
                    nvl(jc.organizational_area_code, '-1')
                    <> nvl(jp.organizational_area_code, '-1')
                then 1
                else 0
            end organizational_area_flag,
            case
                when nvl(jc.global_grade_code, '-1') <> nvl(jp.global_grade_code, '-1')
                then 1
                else 0
            end global_grade_flag,
            case
                when nvl(jc.job_role_code, '-1') <> nvl(jp.job_role_code, '-1')
                then 1
                else 0
            end job_role_flag,
            case
                when
                    nvl(jc.specialization_code, '-1')
                    <> nvl(jp.specialization_code, '-1')
                then 1
                else 0
            end specialization_flag,
            case
                when
                    nvl(jc.professional_field_code, '-1')
                    <> nvl(jp.professional_field_code, '-1')
                then 1
                else 0
            end professional_field_flag,
            case
                when nvl(pc.location_code, '-1') <> nvl(pp.location_code, '-1')
                then 1
                else 0
            end location_flag,
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
    mobility_int as (
        select job_sk as job_sk, 'To' type, user_id, effective_date, previous_job_sk
        from mobility_data
        union all
        select
            previous_job_sk as job_sk,
            'From' type,
            user_id,
            effective_date,
            job_sk as previous_job_sk
        from mobility_data
    )
select hash(job_sk, type) as mobility_sk, *
from mobility_int
