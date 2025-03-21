{{ config(materialized="table", transient=false) }}

select *
from {{ source("QCK_SOURCE_TABLES", "DIM_PROFESSIONAL_FIELD") }}
