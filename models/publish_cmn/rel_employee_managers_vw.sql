select distinct
    -- REEM_EMPLOYEE_MANAGER_KEY_REEM as S_KEY,
    reem_employee_profile_sk_ddep as s_key,
    reem_employee_profile_key_ddep as employee_id,
    nvl(reem_fk_manager_key_ddep, '-1') as manager_id,
    ddep_first_name_ddep as manager_first_name,
    ddep_last_name_ddep as manager_last_name,
    ddep_email_ddep as manager_email,
    reem_fk_hr_manager_key_ddep as hr_manager_id,
    reem_hr_manager_first_name_ddep as hr_manager_first_name,
    reem_hr_manager_last_name_ddep as hr_manager_last_name,
    reem_hr_manager_email_ddep as hr_manager_email,
    reem_fk_matrix_manager_key_ddep as matrix_manager_id,
    reem_matrix_manager_first_name_ddep as matrix_manager_first_name,
    reem_matrix_manager_last_name_ddep as matrix_manager_last_name,
    reem_matrix_manager_email_ddep as matrix_manager_email
from {{ ref("rel_employee_managers") }}
inner join
    {{ ref("dim_employee_profile_vw") }} on reem_employee_profile_sk_ddep = employee_key
left outer join
    {{ ref("dim_employee_profile") }} manager
    on nvl(reem_fk_manager_key_ddep, '-1') = manager.ddep_employee_id_ddep
order by 2
