{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
select
    f.headcount_month_sk month_sk,
    f.headcount_type_code,
    f.is_using_remote_flag,
    'WW' org_level,
    'WW' org_value,
    count(distinct f.user_id) as cnt_user,
    sum(working_days) as agg_time_quantity_in_day_nm
from {{ ref("fact_time_management_presence") }} f
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
group by all
union all
select
    f.headcount_month_sk month_sk,
    f.headcount_type_code,
    f.is_using_remote_flag,
    'Zone' org_level,
    nvl(do.geographic_zone_code, '-1') org_value,
    count(distinct f.user_id) as cnt_user,
    sum(working_days) as agg_time_quantity_in_day_nm
from {{ ref("fact_time_management_presence") }} f
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
group by all
union all
select
    f.headcount_month_sk month_sk,
    f.headcount_type_code,
    f.is_using_remote_flag,
    'Country' org_level,
    nvl(do.country_code, '-1') org_value,
    count(distinct f.user_id) as cnt_user,
    sum(working_days) as agg_time_quantity_in_day_nm
from {{ ref("fact_time_management_presence") }} f
join {{ ref("dim_organization_v1") }} do using (organization_sk)
left join {{ ref("dim_employee_v1") }} de using (employee_sk)
group by all
order by 1 desc
