select distinct ddep_division_code_ddep as division_code, ddep_division_ddep as division
from {{ ref("dim_employee_profile") }}
where ddep_division_code_ddep != ''
