select

    posj_pk_orgn as org_key,
    posj_pk_joar as joba_architecture_key,
    posj_dt_begin_posj as begin_date,
    posj_dt_end_posj as end_date

from {{ ref("dim_position_job_architecture_snapshot") }}
