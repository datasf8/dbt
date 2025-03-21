select
    dpir_pk_position_in_range_dpir as position_in_range_key,
    dpir_id_position_in_range_dpir as position_in_range_id,
    dpir_lb_position_in_range_dpir as position_in_range_name,
    dpir_creation_date_dpir as position_in_range_creation_date,
    dpir_modification_date_dpir as position_in_range_modification_date
from {{ ref("dim_position_in_range_snapshot") }}