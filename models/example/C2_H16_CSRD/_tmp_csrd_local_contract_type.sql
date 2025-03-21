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
    local_contract_type_cte as (

        select
            *,
            rank() over (order by user_id, dt_begin) rk,
            rank() over (order by user_id, dt_begin) + 1 rk_1

        from
            (
                select
                    key_position_type.user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position_type.key_position_type_id,
                    local_contract.local_contract_type_id,
                    begda,
                    endda,
                    case
                        when
                            begda >= coalesce(
                                local_contract.local_contract_type_start_date,
                                '1900-01-01'
                            )
                        then begda

                        else
                            coalesce(
                                local_contract.local_contract_type_start_date,
                                '1900-01-01'
                            )
                    end as dt_begin,
                    case
                        when
                            endda <= coalesce(
                                local_contract.local_contract_type_end_date,
                                '9999-12-31'
                            )
                        then endda
                        else
                            coalesce(
                                local_contract.local_contract_type_end_date,
                                '9999-12-31'
                            )
                    end as dt_end

                from {{ ref("_tmp_csrd_key_position_type") }} key_position_type
                join
                    job_information_v1 job_info_init
                    on key_position_type.user_id = job_info_init.user_id
                    and key_position_type.dt_begin <= job_info_init.job_end_date
                    and key_position_type.dt_end >= job_info_init.job_start_date

                left outer join
                    {{ ref("local_contract_type_v1") }} local_contract
                    on job_info_init.local_contract_type_id
                    = local_contract.local_contract_type_id
                    and job_info_init.job_start_date
                    <= local_contract.local_contract_type_end_date
                    and job_info_init.job_end_date
                    >= local_contract.local_contract_type_start_date

            )
    ),
    local_contract_type_intervals as (

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
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from local_contract_type_cte
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
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from local_contract_type_cte beg_inter
                inner join
                    local_contract_type_cte end_inter
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
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from local_contract_type_cte
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
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    ),
    local_contract_type_indicator as (
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
            begda,
            endda,
            dt_begin,
            dt_end

        from local_contract_type_cte

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
            null as local_contract_type_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from local_contract_type_intervals
    )
select *
from local_contract_type_indicator