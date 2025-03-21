{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
select *
from
    (
        with
            numer as (
                select *
                from
                    (
                        select
                            f.time_month_sk month_sk,
                            f.headcount_type_code,
                            'WW' org_level,
                            'WW' org_value,
                            category,
                            sum(time_quantity_in_day) as agg_time_quantity_in_day_nm
                        from {{ ref("fact_time_management_remote") }} f
                        join {{ ref("dim_organization_v1") }} do using (organization_sk)
                        left join {{ ref("dim_employee_v1") }} de using (employee_sk)
                        group by all
                        order by 1 desc
                    )
            ),
            denom as (
                select *
                from
                    (
                        select
                            f.time_month_sk month_sk,
                            f.headcount_type_code,
                            'WW' org_level,
                            'WW' org_value,
                            category,
                            user_id,
                            time_type_code,
                            sum(time_quantity_in_day) as agg_time_quantity_in_day
                        from {{ ref("fact_time_management_remote") }} f
                        join {{ ref("dim_organization_v1") }} do using (organization_sk)
                        left join {{ ref("dim_employee_v1") }} de using (employee_sk)
                        group by all
                        order by 1 desc
                    )
            )
        select
            d.month_sk,
            d.headcount_type_code,
            d.org_level,
            d.org_value,
            d.category,
            count(d.user_id) user_cnt,
            sum(d.agg_time_quantity_in_day) time_quantity_in_day
        from numer n
        join
            denom d
            on n.month_sk = d.month_sk
            and n.headcount_type_code = d.headcount_type_code
            and n.org_level = d.org_level
            and n.org_value = d.org_value
            and n.category = d.category
        group by all
    )
union all
select *
from
    (
        with
            numer as (
                select *
                from
                    (
                        select
                            f.time_month_sk month_sk,
                            f.headcount_type_code,
                            'Zone' org_level,
                            nvl(do.geographic_zone_code, '-1') org_value,
                            category,
                            sum(time_quantity_in_day) as agg_time_quantity_in_day_nm
                        from {{ ref("fact_time_management_remote") }} f
                        join {{ ref("dim_organization_v1") }} do using (organization_sk)
                        left join {{ ref("dim_employee_v1") }} de using (employee_sk)
                        group by all
                        order by 1 desc
                    )
            ),
            denom as (
                select *
                from
                    (
                        select
                            f.time_month_sk month_sk,
                            f.headcount_type_code,
                            'Zone' org_level,
                            nvl(do.geographic_zone_code, '-1') org_value,
                            category,
                            user_id,
                            time_type_code,
                            sum(time_quantity_in_day) as agg_time_quantity_in_day
                        from {{ ref("fact_time_management_remote") }} f
                        join {{ ref("dim_organization_v1") }} do using (organization_sk)
                        left join {{ ref("dim_employee_v1") }} de using (employee_sk)
                        group by all
                        order by 1 desc
                    )
            )
        select
            d.month_sk,
            d.headcount_type_code,
            d.org_level,
            d.org_value,
            d.category,
            count(d.user_id) user_cnt,
            sum(d.agg_time_quantity_in_day) time_quantity_in_day
        from numer n
        join
            denom d
            on n.month_sk = d.month_sk
            and n.headcount_type_code = d.headcount_type_code
            and n.org_level = d.org_level
            and n.org_value = d.org_value
            and n.category = d.category
        group by all
    )
union all
select *
from
    (
        with
            numer as (
                select *
                from
                    (
                        select
                            f.time_month_sk month_sk,
                            f.headcount_type_code,
                            'Country' org_level,
                            nvl(do.country_code, '-1') org_value,
                            category,
                            sum(time_quantity_in_day) as agg_time_quantity_in_day_nm
                        from {{ ref("fact_time_management_remote") }} f
                        join {{ ref("dim_organization_v1") }} do using (organization_sk)
                        left join {{ ref("dim_employee_v1") }} de using (employee_sk)
                        group by all
                        order by 1 desc
                    )
            ),
            denom as (
                select *
                from
                    (
                        select
                            f.time_month_sk month_sk,
                            f.headcount_type_code,
                            'Country' org_level,
                            nvl(do.country_code, '-1') org_value,
                            category,
                            user_id,
                            time_type_code,
                            sum(time_quantity_in_day) as agg_time_quantity_in_day
                        from {{ ref("fact_time_management_remote") }} f
                        join {{ ref("dim_organization_v1") }} do using (organization_sk)
                        left join {{ ref("dim_employee_v1") }} de using (employee_sk)
                        group by all
                        order by 1 desc
                    )
            )
        select
            d.month_sk,
            d.headcount_type_code,
            d.org_level,
            d.org_value,
            d.category,
            count(d.user_id) user_cnt,
            sum(d.agg_time_quantity_in_day) time_quantity_in_day
        from numer n
        join
            denom d
            on n.month_sk = d.month_sk
            and n.headcount_type_code = d.headcount_type_code
            and n.org_level = d.org_level
            and n.org_value = d.org_value
            and n.category = d.category
        group by all
    )
