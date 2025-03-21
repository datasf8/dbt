{{ config(materialized="table", transient=true) }}

with
    geographic_zone_cte as (

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
                    country.company_id,
                    country.country_id,
                    geographic_zone.geographic_zone_id,
                    dt_begin as begda,
                    dt_end as endda,
                    case
                        when
                            country.dt_begin >= coalesce(
                                geographic_zone.geographic_zone_start_date, '1900-01-01'
                            )
                        then country.dt_begin

                        else
                            coalesce(
                                geographic_zone.geographic_zone_start_date, '1900-01-01'
                            )
                    end as dt_begin,
                    case
                        when
                            country.dt_end <= coalesce(
                                geographic_zone.geographic_zone_end_date, '9999-12-31'
                            )
                        then country.dt_end
                        else
                            coalesce(
                                geographic_zone.geographic_zone_end_date, '9999-12-31'
                            )
                    end as dt_end

                from {{ ref("_tmp_csrd_country") }} country
                join
                    {{ ref("company_v1") }} company_v1
                    on country.company_id = company_v1.company_id

                left outer join
                    {{ ref("geographic_zone_v1") }} geographic_zone
                    on company_v1.geographic_zone_code
                    = geographic_zone.geographic_zone_code
                    and country.dt_begin <= geographic_zone.geographic_zone_end_date
                    and country.dt_end >= geographic_zone.geographic_zone_start_date

            )
    ),
    geographic_zone_intervals as (

        select

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
                    begda,
                    endda,
                    begda as dt_begin,
                    min(dateadd(day, -1, dt_begin)) as dt_end

                from geographic_zone_cte
                group by
                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
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
                    beg_inter.begda,
                    beg_inter.endda,
                    case
                        when beg_inter.dt_end <> '9999-12-31'
                        then dateadd(day, 1, beg_inter.dt_end)
                        else beg_inter.dt_end
                    end dt_begin,
                    dateadd(day, -1, end_inter.dt_begin) as dt_end

                from geographic_zone_cte beg_inter
                inner join
                    geographic_zone_cte end_inter
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
                    begda,
                    endda,
                    case
                        when max(dt_end) <> '9999-12-31'
                        then dateadd(day, 1, max(dt_end))
                        else max(dt_end)
                    end dt_begin,
                    endda dt_end
                from geographic_zone_cte
                group by

                    user_id,
                    termination_date,
                    --headcount_present_flag,
                    cost_center_id,
                    business_unit_id,
                    company_id,
                    country_id,
                    begda,
                    endda
            ) intervals
        where dt_begin <= dt_end and dt_begin <> '9999-12-31'

    ),
    geographic_zone_indicator as (
        select distinct

            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            geographic_zone_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from geographic_zone_cte

        union
        select
            user_id,
            termination_date,
            --headcount_present_flag,
            cost_center_id,
            business_unit_id,
            company_id,
            country_id,
            null as geographic_zone_id,
            begda,
            endda,
            dt_begin,
            dt_end

        from geographic_zone_intervals
    )
select *
from geographic_zone_indicator 