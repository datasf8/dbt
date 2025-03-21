{{ config(materialized="table", transient=true) }}


with
    termination as (
        select distinct user_id, mobility_date  as mobility_date
        from {{ ref("job_mobility") }}
        where mobility_type = 'TERMINATION'

    ),
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
    -- Cost Centers. 
    job_info_cte as (
        select
            job_info.user_id,
            job_info.job_start_date,
            job_info.job_end_date,
            (termination_date.mobility_date - 1) as termination_date  -- ,
        -- headcount.headcount_present_flag
        from job_information_v1 job_info
        left outer join
            termination termination_date
            on job_info.user_id = termination_date.user_id
            and termination_date.mobility_date between job_info.job_start_date and job_info.job_end_date
        where 1 = 1
    -- and job_info.user_id = '00403074'           
    ),
    cost_center_cte as (
        select
            *,
            rank() over (order by user_id, dt_begin) rk,
            rank() over (order by user_id, dt_begin) + 1 rk_1
        from
            (
                select
                    job_info.user_id,
                    cost_center.cost_center_id,
                    job_info.job_start_date as begda,
                    job_info.job_end_date as endda,
                    termination_date,
                    --headcount_present_flag,
                    case
                        when
                            job_info.job_start_date >= coalesce(
                                cost_center.cost_center_start_date, '1900-01-01'
                            )
                        then job_info.job_start_date
                        else coalesce(cost_center.cost_center_start_date, '1900-01-01')
                    end as dt_begin,
                    case
                        when
                            job_info.job_end_date
                            <= coalesce(cost_center.cost_center_end_date, '9999-12-31')
                        then job_info.job_end_date
                        else coalesce(cost_center.cost_center_end_date, '9999-12-31')
                    end as dt_end
                from job_info_cte job_info

                left outer join
                    job_information_v1 job_info_init
                    on job_info.user_id = job_info_init.user_id
                    and job_info.job_start_date <= job_info_init.job_end_date
                    and job_info.job_end_date >= job_info_init.job_start_date

                left join
                    {{ ref("cost_center_v1") }} cost_center
                    on job_info_init.cost_center_code = cost_center.cost_center_code
                    and job_info_init.job_start_date <= cost_center.cost_center_end_date
                    and job_info_init.job_end_date >= cost_center.cost_center_start_date
            )
    ),
    cost_center_intervals as (

        select

            user_id,
            termination_date,
            --headcount_present_flag,
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
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, cost_center_cte.dt_begin)) as dt_end

                from cost_center_cte
                group by user_id, termination_date, /*headcount_present_flag,*/ begda, endda

                union
                -- intervalles
                select

                    beg_inter.user_id,
                    beg_inter.termination_date,
                    --beg_inter.headcount_present_flag,
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from cost_center_cte beg_inter
                inner join
                    cost_center_cte end_inter
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
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from cost_center_cte
                group by user_id, termination_date, /*headcount_present_flag, */ begda, endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    ),
    cost_center_indicator as (
        select distinct

            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from cost_center_cte
        -- Ajout des faits sans mesures
        union
        select
            user_id,
            termination_date,
            --headcount_present_flag,
            null as cost_center_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from cost_center_intervals
    )
select *
from cost_center_indicator