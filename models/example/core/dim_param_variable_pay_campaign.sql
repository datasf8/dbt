{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    variable_pay_campaign as (

        select *
        from
            (
                select variable_pay_code, variable_pay_label, year, dbt_valid_from

                from {{ ref("stg_param_variable_pay_campaign") }}
                where dbt_valid_to is null
            ) a
        left outer join
            (
                select
                    variable_pay_code as min_variable_pay_code,
                    min(dbt_updated_at) as creation_date
                from {{ ref("stg_param_variable_pay_campaign") }}
                group by variable_pay_code
            ) b
            on a.variable_pay_code = b.min_variable_pay_code
    ),
    surrogate_key as (
        select p.*, hash(variable_pay_code) as sk_variable_pay_code
        from variable_pay_campaign p
    )
select
    sk_variable_pay_code as dpvp_pk_variable_pay_campaign_dpvp,
    variable_pay_code as dpvp_id_variable_pay_campaign_dpvp,
    variable_pay_label as dpvp_lb_label_dpvp,
    year as dpvp_variable_pay_campaign_year_dpvp,
    creation_date as dpvp_creation_date_dpvp,
    dbt_valid_from as dpvp_modification_date_dpvp
from surrogate_key
union
select '-1',NUll,Null,Null,NUll,Null
