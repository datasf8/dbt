{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    functional_area_v1 as (
        select functional_area_id, functional_area_code, functional_area_name_en
        from {{ ref("functional_area_v1") }}
        where functional_area_start_date <= current_date()
        qualify
            row_number() over (
                partition by functional_area_code
                order by functional_area_start_date desc
            )
            = 1
        union all
        select null, null, null
    ),
    organizational_area_v1 as (
        select
            organizational_area_id,
            organizational_area_code,
            organizational_area_name_en
        from {{ ref("organizational_area_v1") }}
        where organizational_area_start_date <= current_date()
        qualify
            row_number() over (
                partition by organizational_area_code
                order by organizational_area_start_date desc
            )
            = 1
        union all
        select null, null, null
    ),
    brand_v1 as (
        select brand_id, brand_code, brand_name_en
        from {{ ref("brand_v1") }}
        where brand_start_date <= current_date()
        qualify
            row_number() over (partition by brand_code order by brand_start_date desc)
            = 1
        union all
        select null, null, null
    ),
    employee_group_v1 as (
        select employee_group_id, employee_group_code, employee_group_name_en
        from {{ ref("employee_group_v1") }}
        where employee_group_start_date <= current_date()
        qualify
            row_number() over (
                partition by employee_group_code order by employee_group_start_date desc
            )
            = 1
        union all
        select null, null, null
    ),
    job_level_v1 as (
        select job_level_id, job_level_code, job_level_name_en
        from {{ ref("job_level_v1") }}
        where job_level_start_date <= current_date()
        qualify
            row_number() over (
                partition by job_level_code order by job_level_start_date desc
            )
            = 1
        union all
        select null, null, null
    )
select
    hash(
        functional_area_code,
        organizational_area_code,
        brand_code,
        employee_group_code,
        job_level_code
    ) as agg_jobinfo_sk,
    fa.*,
    functional_area_name_en
    || ' ('
    || functional_area_code
    || ')' as functional_area_name_label,
    oa.*,
    organizational_area_name_en
    || ' ('
    || organizational_area_code
    || ')' as organizational_area_name_label,
    b.*,
    brand_name_en || ' (' || brand_code || ')' as brand_name_label,
    eg.*,
    employee_group_name_en
    || ' ('
    || employee_group_code
    || ')' as employee_group_name_label,
    jl.*,
    job_level_name_en || ' (' || job_level_code || ')' as job_level_name_label
from functional_area_v1 fa
join organizational_area_v1 oa
join brand_v1 b
join employee_group_v1 eg
join job_level_v1 jl
