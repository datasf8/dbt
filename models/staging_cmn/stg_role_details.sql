{{ config(materialized="table", transient=false) }}

select *
from {{ source("landing_tables_CMN", "ROLE_DETAILS") }}
