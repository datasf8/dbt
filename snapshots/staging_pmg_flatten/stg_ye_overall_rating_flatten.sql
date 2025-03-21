{% snapshot stg_ye_overall_rating_flatten %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}
   
/*select distinct
TRIM(VALUE:"formDataId")AS FORMDATA_ID,
TRIM(VALUE:"formContentId") as FORMCONTENT_ID,
TRIM(VALUE:"rating") AS YE_OR_RATING
from {{ ref('stg_ye_overall_rating') }}
,lateral flatten ( input => src)
where dbt_valid_to is null
*/

select * from (
select distinct
TRIM(VALUE:"formDataId")AS FORMDATA_ID,
TRIM(VALUE:"formContentId") as FORMCONTENT_ID,
TRIM(VALUE:"rating") AS YE_OR_RATING,
row_number() over (partition by formdata_id order by to_date(dbt_valid_from) desc,FILE_NAME desc) as RK
from {{ ref('stg_ye_overall_rating') }}
,lateral flatten ( input => src)
  where dbt_valid_to is null
 ) where RK=1 

{% endsnapshot %}





