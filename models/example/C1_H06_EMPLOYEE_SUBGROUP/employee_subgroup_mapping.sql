{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    externalcode as employee_subgroup_code,
    esg.employee_subgroup_id,
    cust_employeegroup_externalcode as employee_group_code,
    eg.employee_group_id
from {{ ref("stg_cust_employee_subgroup_flatten") }}
left join {{ ref("employee_subgroup") }} esg on externalcode = employee_subgroup_code
left join
    {{ ref("employee_group") }} eg
    on cust_employeegroup_externalcode = eg.employee_group_code
where dbt_valid_to is null
qualify
    row_number() over (
        partition by externalcode, cust_employeegroup_externalcode
        order by employee_subgroup_start_date desc, employee_group_start_date desc
    )
    = 1
