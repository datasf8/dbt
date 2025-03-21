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
    f.position_month_sk month_sk,
    collate('WW', 'en-ci') org_level,
    'WW' org_value,
    count_if(open_position_flag = true) open_position_cnt,
    nvl(sum(days_to_recruit), 0) tot_days_to_recruit,
    count(days_to_recruit) recruited_cnt
from {{ ref("fact_position") }} f
join {{ ref("dim_organization_v1") }} do using (organization_sk)
group by 1, 2, 3
union all
select
    f.position_month_sk month_sk,
    'Zone' org_level,
    nvl(do.geographic_zone_code, '') org_value,
    count_if(open_position_flag = true) open_position_cnt,
    nvl(sum(days_to_recruit), 0) tot_days_to_recruit,
    count(days_to_recruit) recruited_cnt
from {{ ref("fact_position") }} f
join {{ ref("dim_organization_v1") }} do using (organization_sk)
group by 1, 2, 3
union all
select
    f.position_month_sk month_sk,
    'Country' org_level,
    nvl(do.country_code, '') org_value,
    count_if(open_position_flag = true) open_position_cnt,
    nvl(sum(days_to_recruit), 0) tot_days_to_recruit,
    count(days_to_recruit) recruited_cnt
from {{ ref("fact_position") }} f
join {{ ref("dim_organization_v1") }} do using (organization_sk)
group by 1, 2, 3
order by 1, 2, 3
