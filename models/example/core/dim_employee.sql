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
    employee as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_employee
        where empl_person_id_external_empl <> '-1'
    )
select
    empl_sk_empl as empl_pk_employee_empl,
    empl_person_id_external_empl,
    empl_user_id_empl,
    empl_person_id_empl,
    empl_first_name_empl,
    empl_last_name_empl,
    empl_date_of_birth_empl,
    empl_age_empl,
    empl_creation_date_empl,
    empl_modification_date_empl,
    empl_dt_begin_empl,
    empl_dt_end_empl,
    empl_assignment_class_empl,
    empl_ethnicity_empl,
    empl_race1_empl,
    empl_ethnicity_calculated_empl
from employee
union
select -1, Null, Null, null, null,null,null,null,null,null,null,null,null, null,null,null
