{% snapshot stg_year_end_int %}
{{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

with Year_End_Conv as
(
  SELECT distinct
  a.FORMDATA_ID,
  a.EMP_ID,
  a.FORM_TITLE,
  b.Sender_ID,
  b.Action,
  b.LMD as YE_LMDATE,
  b.FORMCONTENT_ID as YE_FORMCONTENT_ID,
  b.CONTENT_STEPID,
  c.PG_RATING as YE_PG_RATING,
  c.BG_RATING as YE_BG_RATING,
  d.YE_OR_RATING as YE_OR_RATING,
  e.CONVERSATION_DATE as YE_CONVERSATION_DATE,
  case when e.USER_ID0=a.EMP_ID then e.COMMENTS0 when e.USER_ID1=a.EMP_ID Then e.COMMENTS1 else null end as YE_EMPLOYEE_COMMENTS,
  case when e.USER_ID0 <> a.EMP_ID then e.COMMENTS0 when e.USER_ID1<>a.EMP_ID Then e.COMMENTS1 else null end as YE_MANAGER_COMMENTS,
  e.YE_LMD_EMP as YE_EMPLOYEE_LMDATE,
  e.YE_LMD_MGR as YE_MANAGER_LMDATE
  
  FROM (select * from {{ ref('stg_form_header_flatten') }} where DBT_VALID_TO is null) a
  
   JOIN
  (select * from {{ ref('stg_form_content_flatten') }} where DBT_VALID_TO is null) b
  ON a.FORMDATA_ID=b.FORMDATA_ID
   and b.CONTENT_STEPID='5'
  LEFT OUTER JOIN
 (select * from {{ ref('stg_ye_people_business_goals_flatten') }} where DBT_VALID_TO is null) c
 ON  a.FORMDATA_ID=c.FORMDATA_ID
  
  LEFT OUTER JOIN
 (select * from {{ ref('stg_ye_overall_rating_flatten') }} where DBT_VALID_TO is null) d
 ON  a.FORMDATA_ID=d.FORMDATA_ID
  
  LEFT OUTER JOIN
 (select * from {{ ref('stg_ye_comments_flatten') }} where DBT_VALID_TO is null) e
 ON  a.FORMDATA_ID=e.FORMDATA_ID

  )
SELECT DISTINCT
  
  FORMDATA_ID,
  EMP_ID,
  FORM_TITLE,
  Sender_ID,
  Action,
  YE_FORMCONTENT_ID,
  CONTENT_STEPID,
  YE_PG_RATING,
  YE_BG_RATING,
  YE_OR_RATING,
  YE_MANAGER_COMMENTS,
  YE_EMPLOYEE_COMMENTS,
  YE_CONVERSATION_DATE,
  YE_LMDATE,
  YE_EMPLOYEE_LMDATE,
  YE_MANAGER_LMDATE
 FROM Year_End_Conv

{% endsnapshot %}