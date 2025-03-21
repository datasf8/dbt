{% snapshot stg_form_header_flatten %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}
  
   
select * from(
 select distinct
TRIM(VALUE:"formDataId") AS FORMDATA_ID,
TRIM(VALUE:"formSubjectId") AS EMP_ID,
TRIM(VALUE:"formDataStatus") AS FORMDATA_STATUS,
split_part (TRIM(VALUE:"formTitle"),' for ',1) as FORM_TITLE,
CAST(TRIM(VALUE:"formTemplateId") AS INT ) FORM_TEMPLATE_ID,
row_number() over (partition by FORMDATA_ID order by to_date(dbt_valid_from) desc,FILE_NAME desc) as RK
from {{ ref('stg_form_header') }}
,lateral flatten ( input => src:d:results, OUTER => TRUE)
where dbt_valid_to is null 
)where RK=1
{% endsnapshot %}
