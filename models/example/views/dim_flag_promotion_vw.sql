select
    dfpr_pk_fg_promotion_dfpr as flag_promotion_key,
    dfpr_cd_fg_promotion_dfpr as code_flag_promotion,
    dfpr_lb_fg_promotion_dfpr as label_flag_promotion,
    case
        when label_flag_promotion = 'Only Promotions'
        then 'with'
        when label_flag_promotion = 'Without Promotions'
        then 'without'
        else label_flag_promotion
    end as flag,
    dfpr_sh_lb_fg_promotion_dfpr as short_label,
    dfpr_creation_date_dfpr as creation_date,
    dfpr_modification_date_dfpr as modification_date

from {{ ref("dim_flag_promotion_snapshot") }}
