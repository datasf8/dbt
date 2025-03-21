{{ config(materialized="table", transient=false) }}

with
    emp_hr_manager as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_ec_employee_manager
        where empm_man_type_rel = 'HR Manager'
    )

select
    empm_sk_hr_id_empm as empm_pk_hr_id_empm,
    empm_sk_user_id_empl as empm_pk_user_id_empl,
    empm_sk_rel_user_id_empl as empm_pk_rel_user_id_empl,
    employee_id,
    manager_id,
    empm_man_type_rel,
    empm_dt_begin_rel,
    empm_dt_end_rel,
    empm_man_last_name_empl,
    empm_man_first_name_empl
from emp_hr_manager
union
select '-1', null, null, null, null, null, null, null, null, null
