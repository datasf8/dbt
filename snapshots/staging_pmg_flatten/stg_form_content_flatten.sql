{% snapshot stg_form_content_flatten %}
    {{

        config(
          unique_key= "FORMDATA_ID||'-'||CONTENT_STEPID",
          strategy='check',
          check_cols='all',
          
        )

    }}

select * from(
select distinct
TRIM(VALUE:"formDataId") as FORMDATA_ID,
TRIM(VALUE:"formContentId") as FORMCONTENT_ID,
DATEADD(MS, split_part(split_part(value:"auditTrailLastModified",'(',2),'+',1), '1970-01-01') as LMD ,
TRIM(VALUE:"formContentAssociatedStepId") as CONTENT_STEPID,
TRIM(VALUE:"auditTrailSender") as Sender_ID,
TRIM(VALUE:"auditTrailAction") as Action,
row_number() over (partition by FORMDATA_ID,CONTENT_STEPID order by LMD desc,FILE_NAME desc) as RK
from {{ ref('stg_form_content') }}
,lateral flatten ( input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
)where RK=1
{% endsnapshot %}
