{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    num_den as (
        select
            dir.*,
            num.headcount_type_code,
            num.employee_indicators_referential_id as numerator_id,
            den.employee_indicators_referential_id as denominator_id
        from {{ ref("dim_indicators_referential") }} dir
        join
            {{ ref("employee_indicators_referential") }} num
            on dir.indicators_code = num.employee_indicators_code
        left join
            {{ ref("employee_indicators_referential") }} den
            on dir.denominator_code = den.employee_indicators_code
            and num.headcount_type_code = den.headcount_type_code
    ),
    dates as (
        select d.*, code, id as employee_indicators_referential_id
        from {{ ref("dates_referential") }} d
        join
            (
                select indicators_code as code, numerator_id as id
                from num_den
                union
                select distinct denominator_code as code, denominator_id as id
                from num_den
                where denominator_code is not null
            ) nd
        where
            date_yyyy_mm_dd = date_month_end_date
            and date_id > 20220100
            and (date_month = 12 or date_month = month(add_months(current_date, -1)))
    ),
    src_data as (
        select ei.*
        from {{ ref("fact_hr_cockpit_agg") }} ei
        join
            {{ ref("dim_organization_v1") }} o
            on cost_center_id = organization_sk
            and is_ec_live_flag = true
        join
            {{ ref("one_profile_go_live_dates") }} ld
            on o.company_code = ld.company_code
            and year(date_yyyy_mm_dd) >= year(to_date(ec_go_live_date, 'dd-mm-yyyy'))
    ),
    agg_data as (
        select
            floor(date_id / 100) as month_sk,
            employee_indicators_referential_id,
            nvl(cost_center_id, -1) as organization_sk,
            nvl(location_id, -1) as location_sk,
            nvl(key_position_type_id, -1) as key_position_type_sk,
            nvl(job_role_id, -1) as job_architecture_sk,
            nvl(functional_area_id, -1) as functional_area_sk,
            nvl(organizational_area_id, -1) as organizational_area_sk,
            nvl(brand_id, -1) as brand_sk,
            nvl(employee_group_id, -1) as employee_group_sk,
            nvl(employee_subgroup_id, -1) as employee_subgroup_sk,
            nvl(local_pay_grade_id, -1) as local_pay_grade_sk,
            nvl(group_seniority_range_id, -1) as group_seniority_range_sk,
            nvl(job_seniority_range_id, -1) as job_seniority_range_sk,
            nvl(age_seniority_range_id, -1) as age_seniority_range_sk,
            nvl(legal_gender_id, -1) as legal_gender_sk,
            nvl(all_players_status_id, -1) as all_players_status_sk,
            nvl(flsa_status_id, -1) as flsa_status_sk,
            sum(fact_hr_cockpit_agg_value) agg_value
        from dates
        left join src_data ei using (date_id, employee_indicators_referential_id)
        group by all
    ),
    num_den_data as (
        select
            month_sk,
            indicators_referential_sk,
            headcount_type_code,
            ad.* exclude(month_sk, employee_indicators_referential_id, agg_value),
            agg_value as numerator_value,
            null as denominator_value
        from agg_data ad
        join num_den nd on ad.employee_indicators_referential_id = numerator_id
        union all
        select
            month_sk,
            indicators_referential_sk,
            headcount_type_code,
            ad.* exclude(month_sk, employee_indicators_referential_id, agg_value),
            null as numerator_value,
            agg_value as denominator_value
        from agg_data ad
        join num_den nd on ad.employee_indicators_referential_id = denominator_id
    )
select
    * exclude(numerator_value, denominator_value),
    sum(numerator_value) as numerator_value,
    sum(denominator_value) as denominator_value
from num_den_data
group by all
