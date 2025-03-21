{{ config(materialized='table',unique_key="FORMDATA_ID",transient=false) }}

select
 *
FROM {{ source('landing_tables_PMG', 'FORM_HEADER_DELETED_FULL') }}