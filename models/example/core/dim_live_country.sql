{{
    config(
        materialized="table",
        transient=false,
    )
}}
with
    ec_live as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_live_country

    )
select org_code, org_label
from ec_live
