{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}

with
    gender as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_gender
        where gend_sk_gend <> '-1'
    ),
    gender_sort_cte as (

        select
            gend_sk_gend as gend_pk_gend,
            gend_code_gend,
            gend_label_gend,
            gend_gender_category_gend
        from gender
        union
        select '-1', null, null, 'Others'
    )

select
    gend_pk_gend,
    gend_code_gend,
    gend_label_gend,
    gend_gender_category_gend,
    case
        when gend_gender_category_gend = 'Female'
        then 0
        when gend_gender_category_gend = 'Male'
        then 1
        else 2
    end as  gend_gender_sort_gend
from gender_sort_cte
