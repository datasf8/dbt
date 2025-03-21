{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    emp_details as (
        select
            *,
            nvl(
                group_seniority,
                lag(group_seniority) ignore nulls over (
                    partition by user_id order by employment_details_start_date
                )
            ) as last_group_seniority
        from {{ ref("employment_details_v1") }}  -- where user_id='00807379'
    ),  -- select * from emp_details;
    job_info as (
        select *
        from {{ ref("job_information_v1") }} ji  -- where user_id='00807379'
        left join
            (select distinct user_id, personal_id from emp_details) using (user_id)
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),  -- select * from job_info;
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
            ) as from_position_sk,
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
    ),  -- select * from job_mobility;
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
    ),  -- select * from internal_mobility
    data1 as (
        select
            user_id,
            job_start_date,
            job_end_date,
            job_sk,
            organization_sk,
            position_sk,
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
        from internal_mobility
        union all
        select
            user_id,
            job_start_date,
            job_end_date,
            from_job_sk,
            from_organization_sk,
            from_position_sk,
            'From' type,
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
        from internal_mobility
    ),  -- select * from data1;
    data2 as (
        select
            user_id,
            job_start_date,
            job_end_date,
            type,
            job_sk,
            organization_sk,
            position_sk,
            mobility_type,
            mobility_subtype,
            from_mobility_subtype,
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
                from_mobility_subtype for mobility_type1 in (
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
                    from_brand
                )
            )
        where 'FROM_' || mobility_type = mobility_type1
    ),  -- select * from data2;
    final as (
        select
            im.*,
            ji.company_entry_date,
            ji.job_entry_date,
            ji.job_code,
            ji.brand_code,
            ji.functional_area_code,
            ji.organizational_area_code,
            ji.employee_group_code,
            pos.location_code,
            pos.key_position_type_code,
            hc.headcount_type_code,
            hc.headcount_present_flag,
            legal_gender_code,
            all_player_status_id,
            gg.job_level_id,
            iff(
                ji.employee_group_code = 'EG0008',  -- Expatriate (host)
                nvl(emp.last_group_seniority, emph.last_group_seniority),
                emp.last_group_seniority
            ) group_seniority_date,
            round(
                months_between(ji.job_start_date, ji.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(ji.job_start_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(ji.job_start_date, bi.date_of_birth), 3
            ) age_seniority_months
        from data2 im
        join
            job_info ji
            on im.user_id = ji.user_id
            and im.job_start_date = ji.job_start_date
        join
            {{ ref("headcount_v1") }} hc
            on im.user_id = hc.user_id
            and im.job_start_date = hc.job_start_date
            and hc.headcount_present_flag = 1
        left join {{ ref("biographical_information_v1") }} bi using (personal_id)
        left join
            {{ ref("position_v1") }} pos
            on ji.position_code = pos.position_code
            and ji.job_start_date
            between pos.position_start_date and pos.position_end_date
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
            {{ ref("personal_information_v1") }} per_info
            on emp.personal_id = per_info.personal_id
            and ji.job_start_date
            between personal_info_start_date and personal_info_end_date
        left join
            {{ ref("local_pay_grade_v1") }} lpg
            on ji.local_pay_grade_code = lpg.local_pay_grade_code
            and ji.job_start_date
            between local_pay_grade_start_date and local_pay_grade_end_date
        left join
            {{ ref("global_grade_v1") }} gg
            on lpg.global_grade_code = gg.global_grade_code
            and ji.job_start_date
            between global_grade_start_date and global_grade_end_date
    )  -- select * from final;
select
    to_char(f.job_start_date, 'YYYYMM')::integer date_month_code,
    nvl(organization_sk, -1) as organization_sk,
    nvl(ae.agg_personalinfo_sk, -1) as agg_personalinfo_sk,
    nvl(ja.job_architecture_sk, -1) as job_architecture_sk,
    nvl(aj.agg_jobinfo_sk, -1) as agg_jobinfo_sk,
    nvl(loc.location_sk, -1) as location_sk,
    nvl(kpt.key_position_type_sk, -1) as key_position_type_sk,
    initcap(replace(mobility_type, '_', ' ')) mobility_type,
    mobility_subtype,
    from_mobility_subtype,
    type,
    case
        when headcount_type_code = 'STA'
        then 'Statutory'
        when headcount_type_code = 'ALL'
        then 'All'
        when headcount_type_code = 'REL'
        then 'Real'
        else headcount_type_code
    end as headcount_type_code,
    nvl(jsr.range_sk, -1) as job_seniority_range_sk,
    nvl(gsr.range_sk, -1) as group_seniority_range_sk,
    nvl(asr.range_sk, -1) as age_seniority_range_sk,
    count(*) user_count
from final f
left join
    {{ ref("dim_agg_personalinfo") }} ae
    on nvl(f.legal_gender_code, 'NULL') = nvl(ae.legal_gender_code, 'NULL')
    and nvl(f.all_player_status_id, 'NULL') = nvl(ae.all_players_status_id, 'NULL')
left join
    {{ ref("dim_job_architecture_v1_vw") }} ja
    on f.job_code = ja.job_role_code
    and f.job_start_date
    between job_architecture_start_date and job_architecture_end_date
left join
    {{ ref("dim_agg_jobinfo") }} aj
    on nvl(f.functional_area_code, 'NULL') = nvl(aj.functional_area_code, 'NULL')
    and nvl(f.organizational_area_code, 'NULL')
    = nvl(aj.organizational_area_code, 'NULL')
    and nvl(f.brand_code, 'NULL') = nvl(aj.brand_code, 'NULL')
    and nvl(f.employee_group_code, 'NULL') = nvl(aj.employee_group_code, 'NULL')
    and nvl(f.job_level_id, 'NULL') = nvl(aj.job_level_id, 'NULL')
left join
    {{ ref("dim_key_position_type") }} kpt
    on f.key_position_type_code = kpt.key_position_type_code
    and f.job_start_date
    between key_position_type_start_date and key_position_type_end_date
left join
    {{ ref("dim_location_v1") }} loc
    on f.location_code = loc.location_code
    and f.job_start_date between location_start_date and location_end_date
left join
    {{ ref("dim_range_v1") }} jsr
    on jsr.range_type = 'JOBSEN'
    and job_seniority_months >= jsr.range_start
    and job_seniority_months < jsr.range_end
left join
    {{ ref("dim_range_v1") }} asr
    on asr.range_type = 'AGESEN'
    and age_seniority_months / 12 >= asr.range_start
    and age_seniority_months / 12 < asr.range_end
left join
    {{ ref("dim_range_v1") }} gsr
    on gsr.range_type = 'GRPSEN'
    and group_seniority_months >= gsr.range_start
    and group_seniority_months < gsr.range_end
where mobility_type_flag = 1
group by all
