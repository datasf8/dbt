select
    dpvp_pk_variable_pay_campaign_dpvp as variable_pay_campaign_key,
    dpvp_id_variable_pay_campaign_dpvp as variable_pay_id,
    dpvp_lb_label_dpvp as variable_pay_label,
    dpvp_variable_pay_campaign_year_dpvp as variable_pay_campaign_year,
    dpvp_creation_date_dpvp as creation_date,
    dpvp_modification_date_dpvp as modification_date
from {{ ref("dim_param_variable_pay_campaign_snapshot") }}
