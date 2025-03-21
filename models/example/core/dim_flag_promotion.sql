{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    flag_promotion as (

        select *
        from
            (
                select
                    code_id_increase,
                    code_flag_increase,
                    label_flag_increase,
                    short_label,
                    dbt_valid_from

                from {{ ref("stg_flag_promotion") }}
                where dbt_valid_to is null
            ) a
        left outer join
            (
                select
                    code_id_increase as min_code_id_increase,
                    min(dbt_updated_at) as creation_date
                from {{ ref("stg_flag_promotion") }}
                group by code_id_increase
            ) b
            on a.code_id_increase = b.min_code_id_increase
    ),
    surrogate_key as (
        select p.*, hash(code_flag_increase) as sk_flag_promotion from flag_promotion p
    )
select
    sk_flag_promotion as dfpr_pk_fg_promotion_dfpr,
    code_flag_increase as dfpr_cd_fg_promotion_dfpr,
    label_flag_increase as dfpr_lb_fg_promotion_dfpr,
    short_label as dfpr_sh_lb_fg_promotion_dfpr,
    creation_date as dfpr_creation_date_dfpr,
    dbt_valid_from as dfpr_modification_date_dfpr
from surrogate_key
union
select '-201', 'Masked', 'Masked', 'Masked', null, null
union 
select '-1',Null,Null,Null,Null,Null
