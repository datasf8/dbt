select
    kpos_pk_kpos as position_key,
    kpos_id_kpos as id,
    kpos_code_kpos as code,
    kpos_label_kpos as label,
    kpos_status_kpos as status
from {{ ref("dim_key_position_snapshot") }}
