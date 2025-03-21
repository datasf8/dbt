select distinct ddep_nationality_ddep as nationality
from {{ ref("dim_employee_profile") }}
