{{ config(materialized="table", transient=true) }}

with
    bu_cte as (

        select
            *,
            rank() over (order by user_id, dt_begin) rk,
            rank() over (order by user_id, dt_begin) + 1 rk_1

        from
            (
                select
                    user_id,
                    termination_date,
                    -- , headcount_present_flag   
                    cost_center.cost_center_id,
                    bu.business_unit_id,
                    dt_begin as begda,
                    dt_end as endda,
                    case
                        when
                            cost_center.dt_begin
                            >= coalesce(bu.business_unit_start_date, '1900-01-01')
                        then cost_center.dt_begin

                        else coalesce(bu.business_unit_start_date, '1900-01-01')
                    end as dt_begin,
                    case
                        when
                            cost_center.dt_end
                            <= coalesce(bu.business_unit_end_date, '9999-12-31')
                        then cost_center.dt_end
                        else coalesce(bu.business_unit_end_date, '9999-12-31')
                    end as dt_end

                from {{ ref("_tmp_csrd_cost_center") }} cost_center
                join
                    {{ ref("cost_center_v1") }} cost_center_v1
                    on cost_center.cost_center_id = cost_center_v1.cost_center_id

                left outer join
                    {{ ref("business_unit_v1") }} bu
                    on cost_center_v1.business_unit_code = bu.business_unit_code
                    and cost_center.dt_begin <= bu.business_unit_end_date
                    and cost_center.dt_end >= bu.business_unit_start_date
            )
    ),
    bu_intervals as (

        select

            user_id,
            termination_date,
            -- , headcount_present_flag   
            cost_center_id,
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
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from bu_cte
                group by
                    user_id,
                    termination_date,
                    -- , headcount_present_flag   
                    cost_center_id,
                    begda,
                    endda

                union
                -- intervalles
                select

                    beg_inter.user_id,
                    beg_inter.termination_date,
                    -- ,beg_inter.headcount_present_flag 
                    beg_inter.cost_center_id,
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from bu_cte beg_inter
                inner join
                    bu_cte end_inter
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
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from bu_cte
                group by

                    user_id,
                    termination_date,
                    -- , headcount_present_flag   
                    cost_center_id,
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    ),
    bu_indicator as (
        select distinct

            user_id,
            termination_date,
            -- , headcount_present_flag    
            cost_center_id,
            business_unit_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from bu_cte
        -- Ajout des faits sans mesures
        union
        select
            user_id,
            termination_date,
            -- , headcount_present_flag    
            cost_center_id,
            null as business_unit_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from bu_intervals
    )
select *
from bu_indicator