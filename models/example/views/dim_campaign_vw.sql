select distinct
    dcam_pk_campaign_dcam as campaign_key,
    dcam_id_campaign_dcam as campaign_id,
    dcam_lb_label_dcam as campaign_name,
    dcam_pk_start_date_dcam as start_date,
    dcam_lb_fg_end_date_dcam as end_date,
    dcam_creation_date_dcam as creation_date,
    dcam_modification_date_dcam as modification_date
from {{ ref("dim_campaign_snapshot") }}
