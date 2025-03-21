{% snapshot stg_pe_signature_comments_flatten %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}
select * from (
select distinct
TRIM(VALUE:"formDataId")AS FORMDATA_ID,
TRIM(VALUE:"formContentId")as FORMCONTENT_ID,
TRIM(VALUE:"elementKey") AS ELEMENT_KEY,
TRIM(VALUE:"name") AS NAME,
TRIM(VALUE:"checked") AS CHECKED,
row_number() over (partition by FORMDATA_ID order by to_date(dbt_valid_from) desc,FILE_NAME desc) as RK
from {{ ref('stg_pe_signature_comments') }}
,lateral flatten ( input => src)
where dbt_valid_to is null
) where RK=1 
{% endsnapshot %}