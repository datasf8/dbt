{{ config(materialized='table',transient=false) }}


with COUNTRY_ZONES 
as  
  (
  select distinct
 COUNTRY, 
 ZONES

from {{ source('landing_tables_CMN', 'COUNTRY_ZONES') }}
  )
   select    
   COUNTRY, 
 ZONES
  FROM COUNTRY_ZONES
