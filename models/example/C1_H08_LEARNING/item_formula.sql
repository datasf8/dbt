{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    item_id,
    cpnt_id as item_code,
    cpnt_typ_id as item_type_code,
    fin_var_id as finance_var_id,
    currency_code as currency_code,
    formula as formula,
    is_default as is_default_flag
from {{ ref("stg_pa_cpnt_formula") }} spcf
left join
   (select * from {{ ref("item") }} 
   qualify
    row_number() over (
        partition by item_code, item_type_code
        order by item_data_start_date desc, item_revision_date desc
    )
    = 1 ) i
    on cpnt_id = item_code
    and cpnt_typ_id = item_type_code
    --and rev_dte = item_revision_date
    and TO_DATE (i.item_data_start_date ) <= current_date()
where dbt_valid_to is null
qualify
    row_number() over (
        partition by cpnt_id, cpnt_typ_id, fin_var_id, currency_code
        order by i.item_data_start_date desc,rev_dte desc
    )
    = 1