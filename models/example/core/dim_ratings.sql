{{ config(materialized="table", transient=false) }}
with
    ratings as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.pmg_core_sch.dim_ye_form_ratings
    )

select dyer_sk_dyer as dyer_pk_dyer, dyer_code_dyer, dyer_label_dyer
from ratings
union
select -201, null, 'Masked'
