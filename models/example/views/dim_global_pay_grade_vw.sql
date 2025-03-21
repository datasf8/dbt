select
    gpgr_pk_gpgr as global_pay_grade_key,
    gpgr_code_gpgr as global_pay_grade_code,
    gpgr_label_gpgr as global_pay_grade_label,
    gpgr_dt_begin_gpgr as global_pay_grade_begin_date,
    gpgr_dt_end_gpgr as global_pay_grade_end_date,
    gpgr_status_gpgr as global_pay_grade_status,
    gpgr_level_gpgr as global_pay_grade_level
from {{ ref("dim_global_pay_grade_snapshot") }}
