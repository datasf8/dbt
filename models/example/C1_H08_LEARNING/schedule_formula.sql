{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    cte_schd as (
        select *
        from {{ ref("schedule") }} sc
        qualify
            row_number() over (
                partition by schedule_code 
                order by schedule_data_start_date desc
            )
            = 1
    )
select
    sc.schedule_id as schedule_id,
    schd_id as schedule_code,
    fin_var_id as finance_var_code,
    currency_code as currency_code,
    formula as formula,
    is_default as is_default_flag

from {{ ref("stg_pa_sched_formula") }} sf
left join cte_schd sc on sc.schedule_code = sf.schd_id
where dbt_valid_to is null
