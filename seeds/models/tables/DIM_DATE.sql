 

{{ config(materialized="table", transient=false) }}
with date as (select  * from {{ env_var("DBT_PUB_DB") }}.CMN_PUB_SCH.DIM_DATE)
select *
from date 