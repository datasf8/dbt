{% snapshot stg_sgn_signature_comments_flatten %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}
with stg_sgn_signature_comments as
(
select * from(
select
TRIM (src:d:customElement:results[0]:formDataId) AS FORMDATA_ID,
TRIM (src:d:customElement:results[0]:formContentId) as FORMCONTENT_ID,
case when src:d:customElement:results[0]:elementKey = 'EmpSign' then src:d:customElement:results[0]:checked
 when src:d:customElement:results[1]:elementKey = 'EmpSign' then src:d:customElement:results[1]:checked else NULL end as EMPSIGN,
case when src:d:customElement:results[0]:elementKey = 'ManagerSign' then src:d:customElement:results[0]:checked
 when src:d:customElement:results[1]:elementKey = 'ManagerSign' then src:d:customElement:results[1]:checked else NULL end as MANAGERSIGN,
TRIM (src:d:othersRatingComment:results[0]:comment) AS COMMENTS0,
TRIM (src:d:othersRatingComment:results[1]:comment) AS COMMENTS1,
TRIM (src:d:othersRatingComment:results[0]:userId) AS USER_ID0,
TRIM (src:d:othersRatingComment:results[1]:userId) AS USER_ID1,
--to_DATE(split_part(split_part(null,'(',2),'+',1))  as SGN_LMD_EMP,
--to_DATE(split_part(split_part(null,'(',2),'+',1)) as SGN_LMD_MGR,
--to_DATE(split_part(split_part(null,'(',2),'+',1)) as SGN_EMP_DATE,
--to_DATE(split_part(split_part(null,'(',2),'+',1)) as SGN_MGR_DATE,
NULL  as SGN_LMD_EMP,
NULL as SGN_LMD_MGR,
NULL as SGN_EMP_DATE,
NULL as SGN_MGR_DATE,
row_number() over (partition by FORMDATA_ID order by to_date(dbt_valid_from) desc,FILE_NAME desc) as RK
from {{ ref('stg_sgn_signature_comments') }}
 where dbt_valid_to is null
 ) where RK=1 
 )
 SELECT 
 FORMDATA_ID,
 FORMCONTENT_ID,
 cast(EMPSIGN as BOOLEAN)  as EMPSIGN,
 cast(MANAGERSIGN as BOOLEAN)  as MANAGERSIGN,
 COMMENTS0,
 COMMENTS1,
 USER_ID0,
 USER_ID1,
 SGN_LMD_EMP,
 SGN_LMD_MGR,
 SGN_EMP_DATE,
 SGN_MGR_DATE,
 RK
 FROM stg_sgn_signature_comments
{% endsnapshot %}