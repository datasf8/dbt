{% snapshot stg_compl_form_int %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

with COMPL_FORM_INT as
(
  SELECT distinct
  a.FORMDATA_ID,
  a.EMP_ID,
  a.FORM_TITLE,
  b.Sender_ID,
  b.Action,
  b.LMD as COMPL_LMDATE,
  b.CONTENT_STEPID,
  b.FORMCONTENT_ID as COMPL_FORMCONTENT_ID
  
  
  FROM (select * from {{ ref('stg_form_header_flatten') }} where DBT_VALID_TO is null) a
  
  JOIN
  (select * from {{ ref('stg_form_content_flatten') }} where DBT_VALID_TO is null) b
  ON a.FORMDATA_ID=b.FORMDATA_ID
   and b.CONTENT_STEPID='completed'
  
)

SELECT 
  
  FORMDATA_ID,
  EMP_ID,
  FORM_TITLE,
  Sender_ID,
  Action,
  CONTENT_STEPID,
  COMPL_LMDATE,
  COMPL_FORMCONTENT_ID
  
 FROM COMPL_FORM_INT
  

{% endsnapshot %}