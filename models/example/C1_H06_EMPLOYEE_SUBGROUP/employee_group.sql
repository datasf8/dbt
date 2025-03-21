{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    cust_employeegroup as (
        select
            *,
            iff(
                i.mdfsystemeffectiveenddate in ('9999-12-31'),
                lead(i.effectivestartdate - 1, 1, {d '9999-12-31'}) over (
                    partition by i.externalcode order by i.effectivestartdate
                ),
                i.mdfsystemeffectiveenddate
            ) as c_mdfsystemeffectiveenddate
        from {{ ref("stg_cust_employee_group_flatten") }} i
        where i.dbt_valid_to is null
    )
select
    hash(externalcode, effectivestartdate) as employee_group_id,
    externalcode as employee_group_code,
    effectivestartdate as employee_group_start_date,
    c_mdfsystemeffectiveenddate as employee_group_end_date,
    externalname_defaultvalue as employee_group_name_en,
    nvl(externalname_fr_fr, 'Not Translated FR') as employee_group_name_fr,
    mdfsystemstatus as employee_group_status
from cust_employeegroup
