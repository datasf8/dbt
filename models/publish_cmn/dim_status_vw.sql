select distinct ddep_status_ddep as "STATUS" from {{ ref("dim_employee_profile") }}
