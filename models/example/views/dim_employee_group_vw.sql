select
    emgp_pk_emgp as employee_group_key, emgp_code_emgp as code, emgp_label_emgp as label
from {{ ref("dim_employee_group_snapshot") }}
