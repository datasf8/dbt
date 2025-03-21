select distinct
    dpcac_activity_sk_dpcac as activity_key,
    dcac_activity_key_dcac as activity_key_v1,
    dcac_activity_id_dcac as activity_id,
    dcac_activity_name_dcac as activity_name,
    dcac_activity_status_dcac as status,
    dcac_activity_creation_date_dcac as creation_date,
    dcac_activity_last_modification_date_dcac as last_modification_date
from {{ ref("dim_connect_activities") }}
