{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}

with
    flatten as (
        select distinct
            reem_employee_profile_key_ddep,
            reem_fk_manager_key_ddep,
            reem_manager_first_name_ddep,
            reem_manager_last_name_ddep,
            reem_manager_email_ddep,
            reem_fk_hr_manager_key_ddep,
            reem_hr_manager_first_name_ddep,
            reem_hr_manager_last_name_ddep,
            reem_hr_manager_email_ddep,
            reem_fk_matrix_manager_key_ddep,
            reem_matrix_manager_first_name_ddep,
            reem_matrix_manager_last_name_ddep,
            reem_matrix_manager_email_ddep,
            ddep_employee_profile_sk_ddep
        from {{ ref("stg_employee_manager_flatten") }} semf
        left outer join
            {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_employee_profile dmp
            on dmp.ddep_employee_id_ddep = semf.reem_employee_profile_key_ddep
    ),
    surrogate_key as (
        select
            p.*,
            {{ dbt_utils.surrogate_key("REEM_EMPLOYEE_PROFILE_KEY_DDEP") }} as sk_emp_id
        from flatten p

    )
select
    hash(reem_employee_profile_key_ddep) as reem_employee_manager_sk_reem,
    sk_emp_id as reem_employee_manager_key_reem,
    ddep_employee_profile_sk_ddep as reem_employee_profile_sk_ddep,
    reem_employee_profile_key_ddep,
    reem_fk_manager_key_ddep,
    reem_manager_first_name_ddep,
    reem_manager_last_name_ddep,
    reem_manager_email_ddep,
    reem_fk_hr_manager_key_ddep,
    reem_hr_manager_first_name_ddep,
    reem_hr_manager_last_name_ddep,
    reem_hr_manager_email_ddep,
    reem_fk_matrix_manager_key_ddep,
    reem_matrix_manager_first_name_ddep,
    reem_matrix_manager_last_name_ddep,
    reem_matrix_manager_email_ddep
from surrogate_key
