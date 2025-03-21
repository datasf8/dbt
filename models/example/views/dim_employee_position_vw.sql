select

    empp_pk_empl as emp_position_key,
    empp_pk_orgn as org_key,
    empp_dt_begin_empp as begin_date,
    empp_dt_end_empp as end_date
from {{ ref("dim_employee_position_snapshot") }}
