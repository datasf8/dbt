select distinct
    subjectdomain as subject_domain,
    -- COLONNE1 ,
    flg_emp_man,
    sfrole as sf_role
from {{ ref("dim_param_sf_roles") }}
