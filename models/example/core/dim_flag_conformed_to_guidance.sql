{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    flag_conformed as (

        select *
        from
            (
                select
                    code_id_increase,
                    code_flag_increase,
                    label_flag_increase,
                    dbt_valid_from

                from {{ ref("stg_flag_conformed_to_guidance") }}
                where dbt_valid_to is null
            ) a
        left outer join
            (
                select
                    code_id_increase as min_code_id_increase,
                    min(dbt_updated_at) as creation_date
                from {{ ref("stg_flag_conformed_to_guidance") }}
                group by code_id_increase
            ) b
            on a.code_id_increase = b.min_code_id_increase
    ),
    surrogate_key as (
        select p.*, hash(code_flag_increase) as sk_flag_conformed from flag_conformed p
    )
select
    sk_flag_conformed as dfcg_pk_fg_conformed_to_guidance_dfcg,
    code_flag_increase as dfcg_cd_fg_conformed_to_guidance_dfcg,
    label_flag_increase as dfcg_lb_fg_conformed_to_guidance_dfcg,
    creation_date as dfcg_creation_date_dfcg,
    dbt_valid_from as dfcg_modification_date_dfcg
from surrogate_key
union
select '-1',Null,Null,Null,Null
