{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    flag_increase as (

        select *
        from
            (
                select
                    code_id_increase,
                    code_flag_increase,
                    label_flag_increase,
                    dbt_valid_from

                from {{ ref("stg_flag_increase") }}
                where dbt_valid_to is null
            ) a
        left outer join
            (
                select
                    code_id_increase as min_code_id_increase,
                    min(dbt_updated_at) as creation_date
                from {{ ref("stg_flag_increase") }}
                group by code_id_increase
            ) b
            on a.code_id_increase = b.min_code_id_increase
    ),
    surrogate_key as (
        select p.*, hash(code_flag_increase) as sk_flag_increase from flag_increase p
    )
select
    sk_flag_increase as dfin_pk_fg_increase_dfin,
    code_flag_increase as dfin_cd_fg_increase_dfin,
    label_flag_increase as dfin_lb_fg_increase_dfin,
    creation_date as dfin_creation_date_dfin,
    dbt_valid_from as dfin_modification_date_dfin
from surrogate_key
union
select '-1',Null,Null,Null,Null
