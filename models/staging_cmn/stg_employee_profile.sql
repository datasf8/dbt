{{ config(materialized='table',unique_key='SRC',transient=false) }}

select
 *
FROM  {{ source('landing_tables_CMN', 'EMPLOYEE_PROFILE') }}