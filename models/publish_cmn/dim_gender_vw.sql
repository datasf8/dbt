select distinct ddep_gender_ddep as gender
from {{ ref("dim_employee_profile") }}
where ddep_gender_ddep != ' '
