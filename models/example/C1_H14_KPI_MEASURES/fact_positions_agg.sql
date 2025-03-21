{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        snowflake_warehouse="HRDP_DBT_PREM_WH",
        cluster_by=["date_month_code"],
    )
}}

with
    job_information_1 as (
        select job_ji.*, es.employee_status_name_en
        from
            (
                select *
                from {{ ref("job_information_v1") }}
                qualify
                    row_number() over (
                        partition by user_id, job_start_date
                        order by sequence_number desc
                    )
                    = 1
            ) job_ji
        left outer join
            {{ ref("employee_status_v1") }} es
            on job_ji.employee_status_id = es.employee_status_id
    ),
    job_information as (
        select *
        from job_information_1
        where employee_status_id in (32245, 32238, 32244, 32241, 32236, 32243)
    ),
    jn_pos_ji as (
        select
            dmr.*,
            pv.*,
            ji.user_id,
            ji.job_start_date,
            ji.job_end_date,
            ji.position_code as ji_position_code,
            ji.employee_group_code as ji_employee_group_code,
            ji.employee_status_name_en,
            iff(
                date_month_end_date < current_date(),
                date_month_end_date,
                current_date()
            ) as position_date,
            dateadd(
                day,
                1,
                lag(job_end_date) ignore nulls over (
                    partition by pv.position_code order by date_month_code
                )
            ) as calc_pos_end_date1,
            min(position_start_date) over (
                partition by pv.position_code order by position_start_date
            ) as position_start_date1,
            case
                when calc_pos_end_date1 is null
                then position_start_date1
                else calc_pos_end_date1
            end as calc_pos_end_date,  -- adding this logic to get date if position is not mapped to any user
        from {{ ref("date_month_referential_v1") }} dmr
        join
            {{ ref("position_v1") }} pv
            on dmr.date_month_end_date between position_start_date and position_end_date
            and pv.position_status = 'A'
        left join
            job_information ji
            on ji.position_code = pv.position_code
            and dmr.date_month_end_date between job_start_date and job_end_date
            and pv.position_status = 'A'
        qualify
            row_number() over (
                partition by date_month_end_date, pv.position_code
                order by job_start_date desc
            )
            = 1

    ),
    pos_with_2_month_back as (
        select
            p.*,

            case
                when
                    p.position_to_be_recruited_flag = true
                    and p.position_change_reason_code
                    in ('requestrecruit', 'directhire')
                then 'No'
                when
                    p1.position_to_be_recruited_flag = true
                    and p1.position_change_reason_code
                    in ('requestrecruit', 'directhire')
                then 'No'
                when
                    p2.position_to_be_recruited_flag = true
                    and p2.position_change_reason_code
                    in ('requestrecruit', 'directhire')
                then 'No'
                when
                    p.user_id is not null
                    or p1.user_id is not null
                    or p2.user_id is not null
                then 'No'
                else 'Yes'
            end as is_vacant,
            case
                when is_vacant = 'Yes'
                then datediff(days, p.calc_pos_end_date, p.position_date)
                else 0
            end as is_vacant_total,
            -- case
            -- when p.is_transit_position_flag = 'Y'
            -- then datediff(days, p.calc_pos_end_date, p.position_date)
            -- else 0
            -- end as is_transit_total,
            case
                when
                    p.position_change_reason_code = 'requestrecruit'
                    and p.position_to_be_recruited_flag = true
                    and p.user_id is null
                then 1
                else 0
            end as to_be_recruited,
            case
                when
                    p.position_change_reason_code = 'directhire'
                    and p.position_to_be_recruited_flag = true
                    and p.user_id is null
                then 1
                else 0
            end as direct_hire
        from jn_pos_ji p
        left join
            jn_pos_ji p1
            on p.position_code = p1.position_code
            and add_months(p.position_date, -2) = p1.position_date
        left join
            jn_pos_ji p2
            on p.position_code = p2.position_code
            and add_months(p.position_date, -1) = p2.position_date
    ),
    pos as (

        select
            pv.date_month_code,
            jr.job_role_id,
            sp.specialization_id,
            pf.professional_field_id,
            cs.cost_center_id,
            bu.business_unit_id,
            but.business_unit_type_id as type_of_business_unit_id,
            ar.area_id,
            hd.hr_division_id,
            co.company_id,
            cntry.country_id as country_id,
            fa.functional_area_id,
            oa.organizational_area_id,
            loc.location_id,
            br.brand_id,
            kpt.key_position_type_id,
            pv1_rel.position_id,
            pv.position_status,
            pv.position_to_be_recruited_flag,
            pv.position_change_reason_code,
            pv.position_code,
            pv.employee_group_code,
            pv.employee_status_name_en,
            pv.ji_employee_group_code,
            egv.employee_group_id as position_employee_group_id,
            egv_em.employee_group_id as employee_group_id,

            gz.geographic_zone_id,
            iff(
                date_month_end_date < current_date(),
                date_month_end_date,
                current_date()
            ) as position_date,
            row_number() over (
                partition by pv.position_code order by position_date
            ) as new_positions,
            pv.user_id,
            case
                when
                    exists (
                        select 1
                        from {{ ref("position_right_to_return_v1") }} prtr
                        where
                            position_right_to_return_status = 'A'
                            and pv.position_code = prtr.position_code
                            and pv.position_start_date
                            = prtr.position_right_to_return_start_date
                    )
                then 'Yes'
                else 'No'
            end as has_right_to_return,
            pv.is_vacant,
            pv.is_vacant_total,
            pv.is_transit_position_flag,
            -- pv.is_transit_total,
            case
                when pv.is_transit_position_flag = 'Y'
                then
                    datediff(
                        days,
                        greatest(pv.date_month_start_date, pv.position_start_date),
                        pv.date_month_end_date
                    )
                    + 1
                else 0
            end as in_transit_days,
            case
                when pv.is_transit_position_flag = 'Y'
                then
                    sum(in_transit_days) over (
                        partition by pv.position_code order by pv.date_month_code
                    )
                else 0
            end as calc_in_transit_total_days,
            pv.to_be_recruited,
            pv.direct_hire
        -- pv.*,
        -- ji.*
        from pos_with_2_month_back pv

        left join
            {{ ref("job_role_v1") }} jr
            on pv.job_role_code = jr.job_role_code
            and pv.date_month_end_date between job_role_start_date and job_role_end_date
        left join
            {{ ref("specialization_v1") }} sp
            on jr.specialization_code = sp.specialization_code
            and pv.date_month_end_date
            between specialization_start_date and specialization_end_date
        left join
            {{ ref("professional_field_v1") }} pf
            on sp.professional_field_code = pf.professional_field_code
            and pv.date_month_end_date
            between professional_field_start_date and professional_field_end_date
        left join
            {{ ref("cost_center_v1") }} cs
            on pv.cost_center_code = cs.cost_center_code
            and pv.date_month_end_date
            between cost_center_start_date and cost_center_end_date
        left join
            {{ ref("business_unit_v1") }} bu
            on cs.business_unit_code = bu.business_unit_code
            and pv.date_month_end_date
            between business_unit_start_date and business_unit_end_date

        left join
            {{ ref("business_unit_type_v1") }} but
            on bu.business_unit_type_code = but.business_unit_type_code
            and pv.date_month_end_date
            between business_unit_type_start_date and business_unit_type_end_date
        left join
            {{ ref("area_v1") }} ar
            on bu.area_code = ar.area_code
            and pv.date_month_end_date between area_start_date and area_end_date
        left join
            {{ ref("hr_division_v1") }} hd
            on ar.hr_division_code = hd.hr_division_code
            and pv.date_month_end_date
            between hr_division_start_date and hr_division_end_date
        left join
            {{ ref("company_v1") }} co
            on bu.company_code = co.company_code
            and pv.date_month_end_date between company_start_date and company_end_date
        left join
            {{ ref("geographic_zone_v1") }} gz
            on gz.geographic_zone_code = co.geographic_zone_code
            and pv.date_month_end_date
            between geographic_zone_start_date and geographic_zone_end_date
        left join
            {{ ref("country_v1") }} cntry
            on co.country_code = cntry.country_code
            and pv.date_month_end_date
            between cntry.country_start_date and cntry.country_end_date
        left join
            {{ ref("functional_area_v1") }} fa
            on pv.functional_area_code = fa.functional_area_code
            and pv.date_month_end_date
            between functional_area_start_date and functional_area_end_date
        left join
            {{ ref("organizational_area_v1") }} oa
            on pv.organizational_area_code = oa.organizational_area_code
            and pv.date_month_end_date
            between organizational_area_start_date and organizational_area_end_date
        left join
            {{ ref("location_v1") }} loc
            on pv.location_code = loc.location_code
            and pv.date_month_end_date between location_start_date and location_end_date
        left join
            {{ ref("brand_v1") }} br
            on pv.brand_code = br.brand_code
            and pv.date_month_end_date between brand_start_date and brand_end_date
        left join
            {{ ref("key_position_type_v1") }} kpt
            on pv.key_position_type_code = kpt.key_position_type_code
            and pv.date_month_end_date
            between key_position_type_start_date and key_position_type_end_date
        left join
            {{ ref("employee_group_v1") }} egv
            on pv.employee_group_code = egv.employee_group_code
            and pv.date_month_end_date
            between egv.employee_group_start_date and egv.employee_group_end_date
        left join
            {{ ref("employee_group_v1") }} egv_em
            on pv.ji_employee_group_code = egv_em.employee_group_code
            and pv.date_month_end_date
            between egv_em.employee_group_start_date and egv_em.employee_group_end_date

        left join
            {{ ref("position_relationship_v1") }} pl
            on pv.position_code = pl.position_code
            and pv.date_month_end_date
            between pl.position_relationship_start_date
            and pl.position_relationship_end_date
        left join
            {{ ref("position_v1") }} pv1_rel
            on pv1_rel.position_code = pl.related_position
            and pv.date_month_end_date
            between pv1_rel.position_start_date and pv1_rel.position_end_date

    )

select
    date_month_code,
    position_employee_group_id,

    job_role_id,
    specialization_id,
    professional_field_id,
    cost_center_id,
    business_unit_id,
    type_of_business_unit_id,

    area_id,
    hr_division_id,
    company_id,
    geographic_zone_id::number(38, 0) as geographic_zone_id,
    country_id,
    functional_area_id,
    organizational_area_id,
    location_id,
    brand_id,
    position_status,
    key_position_type_id,
    employee_group_id,
    position_id as hr_position_id,
    count(position_status) as active_positions_number,
    count_if(
        user_id is not null
        and employee_status_name_en
        in ('Suspended', 'Furlough', 'Dormant', 'Paid Leave', 'Active', 'Unpaid Leave')
    ) as active_positions_headcount_number,
    count_if(has_right_to_return = 'Yes') as has_right_to_return_number,
    count_if(is_vacant = 'Yes') as is_vacant_number,
    sum(is_vacant_total) as is_vacant_total_days,
    count_if(new_positions = 1) as new_positions_number,
    count_if(is_transit_position_flag = 'Y') as in_transit_number,
    sum(calc_in_transit_total_days) as in_transit_total_days,
    sum(to_be_recruited) as to_be_recruited_number,
    sum(direct_hire) as direct_hire_number
-- ,case when new_position=1 then 1 else 0 end as new_positions_number
from pos
group by all
