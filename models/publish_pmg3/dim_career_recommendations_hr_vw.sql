select distinct
    dcrh_employee_profile_key_ddep as employee_profile_key,
    dcrh_recommendations_hr_dcrh as career_recommendations_hr,
    dcrh_written_by_dcrh as written_by,
    dcrh_last_modified_date_dcrh as last_modified_date
from {{ ref("dim_career_recommendations_hr") }}
