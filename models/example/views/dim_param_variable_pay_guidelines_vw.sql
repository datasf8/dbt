select
    dvpg_pk_guidelines_dvpg as variable_pay_guideline_key,
    dvpg_fk_variable_pay_campaign_dpvp as variable_pay_campaign_key,
    dvpg_rating_dvpg as rating,
    dvpg_min_guidelines_dvpg as min_guidelines,
    dvpg_max_guidelines_dvpg as max_guidelines,
    dvpg_guideline_range_dvpg as guideline_range,
    dvpg_guideline_rating_label_dvpg as guideline_rating_label
from {{ ref("dim_param_variable_pay_guidelines_snapshot") }}
