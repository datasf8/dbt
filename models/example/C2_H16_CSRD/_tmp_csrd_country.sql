{{ config(materialized="table", transient=true) }}
with
    country_cte as (

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
                    business_unit_id,
                    company.company_id,
                    country.country_id,
                    dt_begin as begda,
                    dt_end as endda,
                    case
                        when
                            company.dt_begin
                            >= coalesce(country.country_start_date, '1900-01-01')
                        then company.dt_begin

                        -- 2024-03-01	9999-12-31    
                        else coalesce(country.country_start_date, '1900-01-01')
                    end as dt_begin,
                    case
                        when
                            company.dt_end
                            <= coalesce(country.country_end_date, '9999-12-31')
                        then company.dt_end
                        else coalesce(country.country_end_date, '9999-12-31')
                    end as dt_end

                from {{ ref("_tmp_csrd_cost_company") }} company
                join
                    {{ ref("company_v1") }} company_v1
                    on company.company_id = company_v1.company_id

                left outer join
                    {{ ref("country_v1") }} country
                    on company_v1.country_code = country.country_code
                    and company.dt_begin <= country.country_end_date
                    and company.dt_end >= country.country_start_date

            )
    ),
    country_intervals as (

        select

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
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from country_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    company_id,
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
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from country_cte beg_inter
                inner join
                    country_cte end_inter
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
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from country_cte
                group by

                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    ),
    country_indicator as (
        select distinct

            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from country_cte

        union
        select
            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            company_id,
            null as country_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from country_intervals
    )
select *
from country_indicator