{{ config(materialized='table',unique_key="FORMDATA_ID" ,transient=false) }}
   
select distinct
TRIM(VALUE:"formDataId") AS FORMDATA_ID,
TRIM(VALUE:"formTemplateId") AS FORM_TEMPLATE_ID,
TRIM(VALUE:"formSubjectId") AS FORM_SUBJECT_ID,
split_part (TRIM(VALUE:"formTitle"),' for ',1) as FORM_TITLE
from {{ ref('stg_form_header_deleted_full') }}
,lateral flatten ( input => src:d:results, OUTER => TRUE)