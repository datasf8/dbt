{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    esg_stg as (
        select *
        from {{ ref("stg_cust_employee_subgroup_flatten") }} i
        where i.dbt_valid_to is null
        qualify
            row_number() over (
                partition by externalcode, effectivestartdate
                order by lastmodifieddatetime desc
            )
            = 1
    ),
    esg_enddate as (
        select
            *,
            least(
                lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
                    partition by externalcode order by effectivestartdate
                ),
                mdfsystemeffectiveenddate
            ) as c_mdfsystemeffectiveenddate
        from esg_stg
    )
select
    hash(externalcode, effectivestartdate) as employee_subgroup_id,
    externalcode as employee_subgroup_code,
    effectivestartdate as employee_subgroup_start_date,
    c_mdfsystemeffectiveenddate as employee_subgroup_end_date,
    externalname_defaultvalue as employee_subgroup_name_en,
    nvl(externalname_fr_fr, 'Not Translated FR') as employee_subgroup_name_fr,
    esg.cust_country_code as country_code,
    c.country_id,
    mdfsystemstatus as employee_subgroup_status
from esg_enddate esg
left join
    {{ ref("country") }} c
    on esg.cust_country_code = c.country_code
    and employee_subgroup_start_date <= country_end_date
    and employee_subgroup_end_date >= country_start_date
qualify
    row_number() over (
        partition by esg.externalcode, esg.effectivestartdate
        order by country_start_date desc
    )
    = 1
