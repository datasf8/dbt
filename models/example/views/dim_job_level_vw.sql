select
    jlvl_pk_jlvl as job_level_key,
    jlvl_id_jlvl as id,
    jlvl_code_jlvl as code,
    jlvl_label_jlvl as label,
    jlvl_status_jlvl as status
from {{ ref("dim_job_level_snapshot") }}
