{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
with
    pos as (
        select
            *,
            effectiveenddate calc_enddate,
            lead(effectivestartdate, 1) over (
                partition by code order by effectivestartdate
            ) next_startdate
        from {{ ref("stg_position_flatten") }}
        where dbt_valid_to is null
    ),
    pos_flat as (
        select
            hash(code, effectivestartdate) as position_id,
            code as position_code,
            effectivestartdate as position_start_date,
            iff(
                calc_enddate >= next_startdate, next_startdate - 1, calc_enddate
            ) position_end_date,
            externalname_defaultvalue as position_name_en,
            nvl(externalname_fr_fr, 'Not Translated FR') as position_name_fr,
            jobcode as job_role_code,
            cust_usjobcode as local_job_code,
            cust_operationalarea as functional_area_code,
            cust_scopetype as organizational_area_code,
            costcenter as cost_center_code,
            location as location_code,
            cust_brand as brand_code,
            cust_localpaygrade as local_pay_grade_code,
            payrange as pay_range_code,
            cust_employeegroup as employee_group_code,
            cust_key_position_level as key_position_type_code,
            cust_prime as position_prime_flag,
            vacant as position_to_be_recruited_flag,
            cust_recruiter as position_recruiter_user_id,
            changereason as position_change_reason_code,
            standardhours::decimal(38, 3) as position_weekly_hours,
            targetfte::decimal(38, 4) as position_fte,
            parentposition_code as higher_position_code,
            cust_intransit as is_transit_position_flag,
            effectivestatus as position_status
        from pos
    ),
    higher as (
        select
            position_id,
            position_code,
            higher_position_code,
            position_start_date,
            position_end_date
        from pos_flat
    ),
    jr as (
        select job_role_code, job_role_end_date, job_role_start_date, job_role_id
        from {{ ref("job_role") }}
    ),
    ljr as (
        select local_job_code, local_job_id, local_job_end_date, local_job_start_date
        from {{ ref("local_job") }}
    ),
    area as (
        select
            functional_area_code,
            functional_area_id,
            functional_area_end_date,
            functional_area_start_date
        from {{ ref("functional_area") }}
    ),
    orgarea as (
        select
            organizational_area_code,
            organizational_area_id,
            organizational_area_end_date,
            organizational_area_start_date
        from {{ ref("organizational_area") }}
    ),
    cc as (

        select
            cost_center_code,
            cost_center_id,
            cost_center_end_date,
            cost_center_start_date
        from {{ ref("cost_center") }}

    ),

    loc as (

        select location_code, location_id, location_end_date, location_start_date
        from {{ ref("location") }}

    ),
    brand as (
        select brand_code, brand_id, brand_end_date, brand_start_date
        from {{ ref("brand") }}

    ),

    lpg as (
        select
            local_pay_grade_code,
            local_pay_grade_id,
            local_pay_grade_end_date,
            local_pay_grade_start_date
        from {{ ref("local_pay_grade") }}

    ),

    pr as (
        select pay_range_code, pay_range_id, pay_range_end_date, pay_range_start_date
        from {{ ref("pay_range") }}
    ),
    empg as (
        select
            employee_group_code,
            employee_group_id,
            employee_group_end_date,
            employee_group_start_date
        from {{ ref("employee_group") }}

    ),
    postyp as (
        select
            key_position_type_code,
            key_position_type_id,
            key_position_type_end_date,
            key_position_type_start_date
        from {{ ref("key_position_type") }}
    )

select
    pos_flat.*,
    jr.job_role_id,
    ljr.local_job_id,
    area.functional_area_id,
    orgarea.organizational_area_id,
    cc.cost_center_id,
    loc.location_id,
    brand.brand_id,
    lpg.local_pay_grade_id,
    pr.pay_range_id,
    empg.employee_group_id,
    postyp.key_position_type_id,
    higher.position_id as higher_position_id

from pos_flat
-- -job role id 
left join
    jr
    on pos_flat.job_role_code = jr.job_role_code
    and pos_flat.position_start_date <= jr.job_role_end_date
    and pos_flat.position_end_date >= jr.job_role_start_date

-- -local_job_id
left join
    ljr
    on pos_flat.local_job_code = ljr.local_job_code
    and pos_flat.position_start_date <= ljr.local_job_end_date
    and pos_flat.position_end_date >= ljr.local_job_start_date

-- -functional_area_id
left join
    area
    on pos_flat.functional_area_code = area.functional_area_code
    and pos_flat.position_start_date <= area.functional_area_end_date
    and pos_flat.position_end_date >= area.functional_area_start_date

-- -organizational_area_id
left join
    orgarea
    on pos_flat.organizational_area_code = orgarea.organizational_area_code
    and pos_flat.position_start_date <= orgarea.organizational_area_end_date
    and pos_flat.position_end_date >= orgarea.organizational_area_start_date

-- -cost_center_id
left join
    cc
    on pos_flat.cost_center_code = cc.cost_center_code
    and pos_flat.position_start_date <= cc.cost_center_end_date
    and pos_flat.position_end_date >= cc.cost_center_start_date

-- -location_id
left join
    loc
    on pos_flat.location_code = loc.location_code
    and pos_flat.position_start_date <= loc.location_end_date
    and pos_flat.position_end_date >= loc.location_start_date

-- -brand_id
left join
    brand
    on pos_flat.brand_code = brand.brand_code
    and pos_flat.position_start_date <= brand.brand_end_date
    and pos_flat.position_end_date >= brand.brand_start_date

-- -local_pay_grade_id
left join
    lpg
    on pos_flat.local_pay_grade_code = lpg.local_pay_grade_code
    and pos_flat.position_start_date <= lpg.local_pay_grade_end_date
    and pos_flat.position_end_date >= lpg.local_pay_grade_start_date

-- -pay_range_id
left join
    pr
    on pos_flat.pay_range_code = pr.pay_range_code
    and pos_flat.position_start_date <= pr.pay_range_end_date
    and pos_flat.position_end_date >= pr.pay_range_start_date

-- -employee_group_id
left join
    empg
    on pos_flat.employee_group_code = empg.employee_group_code
    and pos_flat.position_start_date <= empg.employee_group_end_date
    and pos_flat.position_end_date >= empg.employee_group_start_date

-- key_position_type_id
left join
    postyp  -- HRDP_SDDS_QA_DB.BTDP_DS_C2_H01_ORGANIZATION_STRUCTURE_EU_QA_PRIVATE.KEY_POSITION_TYPE
    on pos_flat.key_position_type_code = postyp.key_position_type_code
    and pos_flat.position_start_date <= postyp.key_position_type_end_date
    and pos_flat.position_end_date >= postyp.key_position_type_start_date

-- higher_position_id
left join
    higher
    on pos_flat.higher_position_code = higher.position_code
    and pos_flat.position_start_date <= higher.position_end_date
    and pos_flat.position_end_date >= higher.position_start_date

qualify
    row_number() over (
        partition by pos_flat.position_id
        order by
            jr.job_role_start_date desc,
            ljr.local_job_start_date desc,
            area.functional_area_start_date desc,
            orgarea.organizational_area_start_date desc,
            cc.cost_center_start_date desc,
            loc.location_start_date desc,
            brand.brand_start_date desc,
            lpg.local_pay_grade_start_date desc,
            pr.pay_range_start_date desc,
            empg.employee_group_start_date desc,
            postyp.key_position_type_start_date desc,
            higher.position_start_date desc

    )
    = 1
