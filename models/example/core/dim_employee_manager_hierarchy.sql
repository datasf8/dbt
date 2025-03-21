{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    emp_manager_hierarchy as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_emp_manager_hierarchy
    -- where empl_sk_employee_id_empl <> '-1' and empl_sk_manager_id_empl <> '-1'
    )

select *
-- empl_sk_employee_id_empl as empl_pk_employee_id_empl,
-- empl_sk_manager_id_empl as empl_pk_manager_id_empl,
-- manager_first_name,
-- manager_last_name,
-- start_date,
-- end_date
from emp_manager_hierarchy
