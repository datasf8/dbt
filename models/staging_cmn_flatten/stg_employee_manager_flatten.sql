{{ config(materialized='table',unique_key='REEM_EMPLOYEE_PROFILE_KEY_DDEP',transient=false) }}
 
 select distinct
 replace(VALUE:"userId",'""','') as REEM_EMPLOYEE_PROFILE_KEY_DDEP,
 replace(VALUE:manager:"userId",'"','') AS  REEM_FK_MANAGER_KEY_DDEP,
 replace(VALUE:"hr":"userId",'"','') AS REEM_FK_HR_MANAGER_KEY_DDEP,
 replace(VALUE:matrixManager:results[0]:"userId",'"','') AS REEM_FK_MATRIX_MANAGER_KEY_DDEP,
 replace(VALUE:manager:"firstName", '""', '') as REEM_MANAGER_FIRST_NAME_DDEP,
 replace(VALUE:manager:"lastName", '""', '') as REEM_MANAGER_LAST_NAME_DDEP,
 replace(VALUE:manager:"email", '""', '') as REEM_MANAGER_EMAIL_DDEP,
 replace(VALUE:matrixManager:results[0]:"firstName", '""', '') as REEM_MATRIX_MANAGER_FIRST_NAME_DDEP,
 replace(VALUE:matrixManager:results[0]:"lastName", '""', '') as REEM_MATRIX_MANAGER_LAST_NAME_DDEP,
 replace(VALUE:matrixManager:results[0]:"email", '""', '') as REEM_MATRIX_MANAGER_EMAIL_DDEP,
 replace(VALUE:"hr":"firstName", '""', '') as REEM_HR_MANAGER_FIRST_NAME_DDEP,
 replace(VALUE:"hr":"lastName", '""', '') as REEM_HR_MANAGER_LAST_NAME_DDEP,
 replace(VALUE:"hr":"email", '""', '') as REEM_HR_MANAGER_EMAIL_DDEP
from {{ ref('stg_employee_manager') }}
,lateral flatten ( input => src:d:results, OUTER => TRUE)