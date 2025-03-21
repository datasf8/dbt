select distinct
    reeu_subject_domain_reeu as subject_domain,
    reeu_access_employee_id_reem as access_employee_id,
    reeu_access_employee_upn_ddep as access_employee_upn,
    reeu_employee_id_ddep as employee_id
from {{ ref("rel_employee_user") }}
