{% snapshot stg_perf_eval_int %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

with PERF_EVAL as
(
  SELECT distinct
  a.FORMDATA_ID,
  a.EMP_ID,
  a.FORMDATA_STATUS,
  a.FORM_TITLE,
  b.Sender_ID,
  b.Action,
  b.LMD as PE_LMDATE,
  b.CONTENT_STEPID,
  b.FORMCONTENT_ID as PE_FORMCONTENT_ID,
  c.PG_RATING as PE_PG_RATING,
  c.BG_RATING as PE_BG_RATING,
  d.OR_RATING as PE_OR_RATING,
  e.COMMENTS as PE_MANAGER_COMMENTS
  
  FROM (select * from {{ ref('stg_form_header_flatten') }} where DBT_VALID_TO is null) a
  
  JOIN
  (select * from {{ ref('stg_form_content_flatten') }} where DBT_VALID_TO is null) b
  ON a.FORMDATA_ID=b.FORMDATA_ID
   and b.CONTENT_STEPID='2'
  LEFT OUTER JOIN
  (select * from {{ ref('stg_pe_people_business_goals_flatten') }} where DBT_VALID_TO is null) c
 ON  a.FORMDATA_ID=c.FORMDATA_ID
  
  LEFT OUTER JOIN
  (select * from {{ ref('stg_pe_overall_rating_flatten') }} where DBT_VALID_TO is null) d
 ON  a.FORMDATA_ID=d.FORMDATA_ID
  
  LEFT OUTER JOIN
  {{ ref('stg_pe_comments_report') }} e
  ON  a.FORMDATA_ID=e.FORMDATA_ID
)

SELECT 
  
  FORMDATA_ID,
  EMP_ID,
  FORM_TITLE,
  FORMDATA_STATUS,
  Sender_ID,
  Action,
  CONTENT_STEPID,
  PE_FORMCONTENT_ID,
  PE_PG_RATING,
  PE_BG_RATING,
  PE_OR_RATING,
  PE_LMDATE,
  PE_MANAGER_COMMENTS
 FROM PERF_EVAL
  

{% endsnapshot %}
