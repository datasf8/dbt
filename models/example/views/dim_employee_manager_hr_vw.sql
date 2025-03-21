select
    empm_pk_hr_id_empm as sk_hr_manager_id,
    empm_pk_user_id_empl as sk_employee_id,
    empm_pk_rel_user_id_empl as sk_manager_id,
    empm_man_type_rel as manager_type,
    empm_dt_begin_rel as manager_start_date,
    empm_dt_end_rel as manager_end_date,
    empm_man_last_name_empl as manager_last_name,
    empm_man_first_name_empl as manager_first_name,
    collate(
       upper( manager_last_name) || ' ' || manager_first_name, 'en-ci'
    ) as manager_full_name
from {{ ref("dim_employee_manager_hr_snapshot") }}
