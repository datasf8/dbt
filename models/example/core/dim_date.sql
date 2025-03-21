{{ config(materialized="table", transient=false) }}
with date as (select distinct * from {{ env_var("DBT_CORE_DB") }}.lrn_core_sch.dim_date)
select *
from date