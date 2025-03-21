{{ config(materialized='table',unique_key="FORMDATA_ID",transient=false) }}

select
 *
FROM {{ source('landing_tables_PMG', 'PE_COMMENTS_REPORT') }}

