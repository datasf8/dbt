select distinct
    dcra_employee_profile_key_ddep as employee_profile_key,
    dcra_career_aspirations_dcra as career_aspirations,
    dcra_last_modified_date_dcra as last_modified_date
from {{ ref("dim_career_aspirations") }}
