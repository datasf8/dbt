select
    dfin_pk_fg_increase_dfin as flag_increase_key,
    dfin_cd_fg_increase_dfin as code_flag_increase,
    dfin_lb_fg_increase_dfin as label_flag_increase,
    collate(
        case
            when label_flag_increase = 'Only Increases'
            then 'with'
            when label_flag_increase = 'Without Increases'
            then 'without'
            else label_flag_increase
        end,
        'en-ci'
    ) as flag,
    collate(
        case
            when code_flag_increase = 'Y'
            then 'yes'
            when code_flag_increase = 'N'
            then 'no'
            else null
        end,
        'en-ci'
    ) as short_label,
    dfin_creation_date_dfin as creation_date,
    dfin_modification_date_dfin as modification_date
from {{ ref("dim_flag_increase_snapshot") }}
