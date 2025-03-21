select
    dfcg_pk_fg_conformed_to_guidance_dfcg as flag_conformed_to_guidance_key,
    dfcg_cd_fg_conformed_to_guidance_dfcg as code_flag_conformed_to_guidance,
    dfcg_lb_fg_conformed_to_guidance_dfcg as label_flag_conformed_to_guidance,
    dfcg_creation_date_dfcg as creation_date,
    dfcg_modification_date_dfcg as modification_date

from {{ ref("dim_flag_conformed_to_guidance_snapshot") }}
