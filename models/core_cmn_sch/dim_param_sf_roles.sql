{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}


with PARAM_SF_ROLES 
as  
  (
  select distinct
 SUBJECTDOMAIN, 
 COLONNE1,
 FLG_EMP_MAN,
 SFROLEID,
 SFROLE,
 SEQ
from {{ source('landing_tables_CMN', 'PARAM_SF_ROLES') }}
  )
   select    
   SUBJECTDOMAIN,
   --COLONNE1,
   FLG_EMP_MAN,
   SFROLEID,
   SFROLE,
   SEQ
  FROM PARAM_SF_ROLES
