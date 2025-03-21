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
    key_position_type_cte as (

        select
            *,
            rank() over (order by user_id, dt_begin) rk,
            rank() over (order by user_id, dt_begin) + 1 rk_1

        from
            (
                select
                    employee_group.user_id,
                    termination_date,
                    -- , headcount_present_flag    
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    key_position.key_position_type_id,
                    dt_begin as begda,
                    dt_end as endda,
                    case
                        when
                            dt_begin >= coalesce(
                                key_position.key_position_type_start_date, '1900-01-01'
                            )
                        then dt_begin

                        else
                            coalesce(
                                key_position.key_position_type_start_date, '1900-01-01'
                            )
                    end as dt_begin,
                    case
                        when
                            dt_end <= coalesce(
                                key_position.key_position_type_end_date, '9999-12-31'
                            )
                        then dt_end
                        else
                            coalesce(
                                key_position.key_position_type_end_date, '9999-12-31'
                            )
                    end as dt_end

                from {{ ref("_tmp_csrd_employee_group") }} employee_group
                join
                    job_information_v1 job_info_init
                    on employee_group.user_id = job_info_init.user_id
                    and employee_group.dt_begin <= job_info_init.job_end_date
                    and employee_group.dt_end >= job_info_init.job_start_date

                left outer join
                    {{ ref("key_position_type_v1") }} key_position
                    on job_info_init.key_position_type_id
                    = key_position.key_position_type_id
                    and job_info_init.job_start_date
                    <= key_position.key_position_type_end_date
                    and job_info_init.job_end_date
                    >= key_position.key_position_type_start_date

            )
    ),
    key_position_type_intervals as (

        select

            user_id,
            termination_date,
            -- , headcount_present_flag    
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            geographic_zone_id,
            employee_group_id,
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
                    -- , headcount_present_flag    
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from key_position_type_cte
                group by
                    user_id,
                    termination_date,
                    -- , headcount_present_flag    
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    begda,
                    endda

                union
                -- intervalles
                select

                    beg_inter.user_id,
                    beg_inter.termination_date,
                    -- ,beg_inter.headcount_present_flag 
                    beg_inter.cost_center_id,
                    beg_inter.business_unit_id,
                    beg_inter.company_id,
                    beg_inter.country_id,
                    beg_inter.geographic_zone_id,
                    beg_inter.employee_group_id,
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from key_position_type_cte beg_inter
                inner join
                    key_position_type_cte end_inter
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
                    -- , headcount_present_flag   
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from key_position_type_cte
                group by
                    user_id,
                    termination_date,
                    -- , headcount_present_flag   
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    geographic_zone_id,
                    employee_group_id,
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    ),
    key_position_type_indicator as (
        select distinct

            user_id,
            termination_date,
            -- , headcount_present_flag    
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

        from key_position_type_cte

        union
        select
            user_id,
            termination_date,
            -- , headcount_present_flag    
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            geographic_zone_id,
            employee_group_id,
            null as key_position_type_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from key_position_type_intervals
    )
select *
from key_position_type_indicator