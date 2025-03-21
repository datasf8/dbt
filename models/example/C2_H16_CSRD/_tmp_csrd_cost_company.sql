{{ config(materialized="table", transient=true) }}

with
    company_cte as (

        select
            *,
            rank() over (order by user_id, dt_begin) rk,
            rank() over (order by user_id, dt_begin) + 1 rk_1

        from
            (
                select
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    bu.business_unit_id,
                    company.company_id,
                    dt_begin as begda,
                    dt_end as endda,
                    case
                        when
                            bu.dt_begin
                            >= coalesce(company.company_start_date, '1900-01-01')
                        then bu.dt_begin

                        else coalesce(company.company_start_date, '1900-01-01')
                    end as dt_begin,
                    case
                        when
                            bu.dt_end
                            <= coalesce(company.company_end_date, '9999-12-31')
                        then bu.dt_end
                        else coalesce(company.company_end_date, '9999-12-31')
                    end as dt_end

                from {{ ref("_tmp_csrd_bu") }} bu
                join
                    {{ ref("business_unit_v1") }} business_unit_v1
                    on bu.business_unit_id = business_unit_v1.business_unit_id
                left outer join
                    {{ ref("company_v1") }} company
                    on business_unit_v1.company_code = company.company_code
                    and bu.dt_begin <= company.company_end_date
                    and bu.dt_end >= company.company_start_date
            )
    ),
    company_intervals as (

        select

            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
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
                   -- headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from company_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
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
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from company_cte beg_inter
                inner join
                    company_cte end_inter
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
                   -- headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from company_cte
                group by

                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    ),
    company_indicator as (
        select distinct

            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            company_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from company_cte

        union
        select
            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            null as company_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from company_intervals
    )
select *
from company_indicator