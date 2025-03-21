{{ config(materialized="table", unique_key="EMPLOYEE_ID", transient=true) }}

select * 
from  {{ source("landing_tables_CMP", "YE_COMPENSATION_YE2023") }}