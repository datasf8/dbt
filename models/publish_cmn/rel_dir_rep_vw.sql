select
    a.reem_employee_profile_key_ddep as emp_id,
    a.reem_fk_manager_key_ddep as manager_id,
    b.reem_fk_manager_key_ddep as second_manager_id

from {{ ref("rel_employee_managers") }} a
left outer join
    {{ ref("rel_employee_managers") }} b
    on a.reem_fk_manager_key_ddep = b.reem_employee_profile_key_ddep
