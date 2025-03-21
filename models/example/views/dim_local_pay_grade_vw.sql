select
    lpgr_pk_lpgr as local_pay_grade_key,
    lpgr_code_lpgr as local_pay_grade_code,
    lpgr_label_lpgr as local_pay_grade_label,
    gpgr_label_gpgr as global_pay_grade_label,
    lpgr_dt_begin_lpgr as local_pay_grade_begin_date,
    lpgr_dt_end_lpgr as local_pay_grade_end_date,
    lpgr_status_lpgr as local_pay_grade_status,
    lpgr_pk_gpgr as global_pay_grade_key,
    lpgr_code_country_lpgr as local_pay_grade_country_code
from {{ ref("dim_local_pay_grade_snapshot") }}
inner join {{ ref("dim_global_pay_grade_snapshot") }} on lpgr_pk_gpgr = gpgr_pk_gpgr
