{{ config(materialized="table", transient=true) }}

with bu_type_cte as (

        select
            *,
            rank() over (order by user_id, dt_begin) rk,
            rank() over (order by user_id, dt_begin) + 1 rk_1

        from
            (
                select
                    base.user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    base.business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    base.key_position_type_id,
                    base.local_contract_type_id,
                    base.employee_status_id,
                    base.legal_gender_id,
                    disability_status_id,
                    dt_begin as begda,
                    dt_end as endda,
                    bu_type.business_unit_type_id,
                    case
                        when
                            dt_begin >= coalesce(
                                bu_type.business_unit_type_start_date,
                                '1900-01-01'
                            )
                        then dt_begin
                        else
                            coalesce(
                                bu_type.business_unit_type_start_date,
                                '1900-01-01'
                            )
                    end as dt_begin,
                    case
                        when
                            dt_end <= coalesce(
                                bu_type.business_unit_type_end_date,
                                '9999-12-31'
                            )
                        then dt_end
                        else
                            coalesce(
                                bu_type.business_unit_type_end_date,
                                '9999-12-31'
                            )
                    end as dt_end

                from
                    {{ ref('_tmp_csrd_disability_status') }} base

left outer join {{ ref('business_unit_v1') }}  bu on base.BUSINESS_UNIT_ID = bu.BUSINESS_UNIT_ID
left outer join {{ ref('business_unit_type_v1') }} bu_type on bu.BUSINESS_UNIT_TYPE_CODE = bu_type.BUSINESS_UNIT_TYPE_CODE
                    and base.dt_begin
                    <= bu_type.business_unit_type_end_date
                    and base.dt_end
                    >= bu_type.business_unit_type_start_date 
            )
    ),
    bu_type_intervals as (

        select

            user_id,
            termination_date,
            --headcount_present_flag,
            personal_id,
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            geographic_zone_id,
            employee_group_id,
            key_position_type_id,
            local_contract_type_id,
            employee_status_id,
            legal_gender_id,
            disability_status_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from
            (

                -- borne inférieure
                select

                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    employee_status_id,
                    legal_gender_id,
                    disability_status_id,
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from bu_type_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    employee_status_id,
                    legal_gender_id,
                    disability_status_id,
                    begda,
                    endda

                union
                -- intervalles
                select

                    beg_inter.user_id,
                    beg_inter.termination_date,
                    --beg_inter.headcount_present_flag,
                    beg_inter.personal_id,
                    beg_inter.cost_center_id,
                    beg_inter.business_unit_id,
                    beg_inter.company_id,
                    beg_inter.country_id,
                    beg_inter.geographic_zone_id,
                    beg_inter.employee_group_id,
                    beg_inter.key_position_type_id,
                    beg_inter.local_contract_type_id,
                    beg_inter.employee_status_id,
                    beg_inter.legal_gender_id,
                    beg_inter.disability_status_id,
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from bu_type_cte beg_inter
                inner join
                    bu_type_cte end_inter
                    on (
                        beg_inter.user_id = end_inter.user_id
                        and beg_inter.begda = end_inter.begda
                        and beg_inter.rk_1 = end_inter.rk
                    )
                union
                -- borne supérieure
                select
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    employee_status_id,
                    legal_gender_id,
                    disability_status_id,
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from bu_type_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    personal_id,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    employee_status_id,
                    legal_gender_id,
                    disability_status_id,
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    )
select distinct

    user_id,
    termination_date,
    --headcount_present_flag,
    personal_id,
    cost_center_id,
    business_unit_id,
    company_id,
    country_id,
    geographic_zone_id,
    employee_group_id,
    key_position_type_id,
    local_contract_type_id,
    employee_status_id,
    legal_gender_id,
    disability_status_id,
    business_unit_type_id,
    begda,
    endda,
    dt_begin,
    dt_end

from bu_type_cte

union
select
    user_id,
    termination_date,
    --headcount_present_flag,
    personal_id,
    cost_center_id,
    business_unit_id,
    company_id,
    country_id,
    geographic_zone_id,
    employee_group_id,
    key_position_type_id,
    local_contract_type_id,
    employee_status_id,
    legal_gender_id,
    disability_status_id,
    null as business_unit_type_id,    
    begda,
    endda,
    dt_begin,
    dt_end

from bu_type_intervals