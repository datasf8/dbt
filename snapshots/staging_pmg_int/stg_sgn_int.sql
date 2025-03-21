{% snapshot stg_sgn_int %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

with SIGN_COMMENTS as
(
  SELECT distinct
  a.FORMDATA_ID,
  a.EMP_ID,
  a.FORM_TITLE,
  b.Sender_ID,
  b.Action,
  b.LMD as SGN_LMDATE,
  b.CONTENT_STEPID,
  b.FORMCONTENT_ID as SGN_FORMCONTENT_ID,
  c.EMPSIGN as SGN_EMP_SIGN,
  c.MANAGERSIGN AS SGN_MANAGER_SIGN,
  case when c.USER_ID0=a.EMP_ID then c.COMMENTS0 when c.USER_ID1=a.EMP_ID Then c.COMMENTS1 else null end as SGN_EMPLOYEE_COMMENTS,
  case when c.USER_ID0=d.REEM_FK_MANAGER_KEY_DDEP then c.COMMENTS0 when c.USER_ID1=d.REEM_FK_MANAGER_KEY_DDEP Then c.COMMENTS1 else null end as SGN_MANAGER_COMMENTS,
  d.REEM_FK_MANAGER_KEY_DDEP as MANAGER_ID,
  c.SGN_LMD_EMP as SGN_LMD_EMP,
  c.SGN_LMD_MGR as SGN_LMD_MGR,
  c.SGN_EMP_DATE as SGN_EMP_DATE,
  c.SGN_MGR_DATE as SGN_MGR_DATE
  
  FROM (select * from {{ ref('stg_form_header_flatten') }} where DBT_VALID_TO is null) a
  
  JOIN
  (select * from {{ ref('stg_form_content_flatten') }} where DBT_VALID_TO is null) b
  ON a.FORMDATA_ID=b.FORMDATA_ID
  and b.CONTENT_STEPID='S1'
  LEFT OUTER JOIN
  (select * from {{ ref('stg_sgn_signature_comments_flatten') }} where DBT_VALID_TO is null) c
 ON  a.FORMDATA_ID=c.FORMDATA_ID
  LEFT OUTER JOIN
  {{ ref('stg_employee_manager_flatten') }} d
  ON a.EMP_ID = d.REEM_EMPLOYEE_PROFILE_KEY_DDEP 
  )
       
    SELECT 
  FORMDATA_ID,
  EMP_ID,
  MANAGER_ID,
  FORM_TITLE,
  Sender_ID,
  Action,
  SGN_FORMCONTENT_ID,
  CONTENT_STEPID,
  SGN_EMP_SIGN,
  SGN_MANAGER_SIGN,
  SGN_EMPLOYEE_COMMENTS,
  SGN_MANAGER_COMMENTS,
  SGN_LMDATE,
  SGN_LMD_EMP,
  SGN_LMD_MGR,
  SGN_EMP_DATE,
  SGN_MGR_DATE
  FROM SIGN_COMMENTS
  
  {% endsnapshot %}