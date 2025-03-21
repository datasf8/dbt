{{ config(materialized="table", transient=true) }}


with
    job_information_v1 as (
        select *
        from {{ ref("job_information_v1") }} job_info

        where 1 = 1
        -- and job_info.user_id = '00039256' 
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    employee_status_cte as (

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
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    base.key_position_type_id,
                    base.local_contract_type_id,
                    employee_status.employee_status_id,
                    dt_begin as begda,
                    dt_end as endda,
                    case
                        when
                            dt_begin >= coalesce(
                                employee_status.employee_status_start_date, '1900-01-01'
                            )
                        then dt_begin

                        else
                            coalesce(
                                employee_status.employee_status_start_date, '1900-01-01'
                            )
                    end as dt_begin,
                    case
                        when
                            dt_end <= coalesce(
                                employee_status.employee_status_end_date, '9999-12-31'
                            )
                        then dt_end
                        else
                            coalesce(
                                employee_status.employee_status_end_date, '9999-12-31'
                            )
                    end as dt_end

                from {{ ref("_tmp_csrd_local_contract_type") }} base
                join
                    job_information_v1 job_info_init
                    on base.user_id = job_info_init.user_id
                    and base.dt_begin <= job_info_init.job_end_date
                    and base.dt_end >= job_info_init.job_start_date

                join
                    {{ ref("employee_status_v1") }}  employee_status
                    on job_info_init.employee_status_id
                    = employee_status.employee_status_id
                    and job_info_init.job_start_date
                    <= employee_status.employee_status_end_date
                    and job_info_init.job_end_date
                    >= employee_status.employee_status_start_date

            )
    ),
    employee_status_intervals as (

        select

            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            geographic_zone_id,
            employee_group_id,
            key_position_type_id,
            local_contract_type_id,
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
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from employee_status_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    begda,
                    endda

                union
                -- intervalles
                select

                    beg_inter.user_id,
                    beg_inter.termination_date,
                    --beg_inter.headcount_present_flag,
                    beg_inter.cost_center_id,
                    beg_inter.business_unit_id,
                    beg_inter.company_id,
                    beg_inter.country_id,
                    beg_inter.geographic_zone_id,
                    beg_inter.employee_group_id,
                    beg_inter.key_position_type_id,
                    beg_inter.local_contract_type_id,
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from employee_status_cte beg_inter
                inner join
                    employee_status_cte end_inter
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
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from employee_status_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type_id,
                    local_contract_type_id,
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    ),
    employee_status_indicator as (
        select distinct

            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            geographic_zone_id,
            employee_group_id,
            key_position_type_id,
            local_contract_type_id,
            employee_status_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from employee_status_cte

        union
        select
            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            geographic_zone_id,
            employee_group_id,
            key_position_type_id,
            local_contract_type_id,
            null as employee_status_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from employee_status_intervals
    )
select *
from employee_status_indicator