{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(de.user_id) as dcpv_comp_plan_sk_ddep,
    de.user_id as comp_planner_user_id,
    de.empl_full_name as comp_planner_full_name
from {{ ref("dim_employee_vw") }} de
where
    exists (
        select 1
        from {{ ref("stg_ye_cpsn_report_int") }} sycr
        where sycr.comp_plan_owner = de.user_id
    )
    or exists (
        select 1
        from {{ ref("stg_variable_pay_global_report") }} sycr
        where sycr.comp_plan_owner = de.user_id
    )
union 
select '-1',Null,Null