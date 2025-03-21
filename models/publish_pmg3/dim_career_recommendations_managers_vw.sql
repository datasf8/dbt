select distinct
    dcrm_employee_profile_key_ddep as employee_profile_key,
    dcrm_career_recommendations_dcrm as career_recommendations,
    dcrm_last_modified_date_dcrm as last_modified_date
from {{ ref("dim_career_recommendations_managers") }}
