select
    emsg_pk_emsg as emp_subgroup_key,
    emsg_code_emsg as code,

    emsg_effectivestartdate_emsg as effective_start_date,
    emsg_label_emsg as subgroup
from {{ ref("dim_employee_subgroup_snapshot") }}
