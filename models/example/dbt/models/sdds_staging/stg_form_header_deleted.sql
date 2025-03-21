{{ config(materialized='table',
schema="SDDS_STG_SCH",
transient=false) }}

select
 *
FROM {{ source('landing_tables_SDDS', 'FORM_HEADER_DELETED_FULL') }}