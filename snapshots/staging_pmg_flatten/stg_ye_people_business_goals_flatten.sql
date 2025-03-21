{% snapshot stg_ye_people_business_goals_flatten %}
    {{

        config(
          unique_key= "FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

/*select distinct 
trim (src:d:overallObjRating:formDataId) AS FORMDATA_ID,
trim (src:d:overallObjRating:formContentId) as FORMCONTENT_ID,
trim (src:d:overallCompRating:rating) as PG_RATING,
trim (src:d:overallObjRating:rating) as BG_RATING
from {{ ref('stg_ye_people_business_goals') }}
where dbt_valid_to is null
*/
select * from
(                                                                                                
 select distinct 
trim (src:d:overallObjRating:formDataId) AS FORMDATA_ID,
trim (src:d:overallObjRating:formContentId) as FORMCONTENT_ID,
trim (src:d:overallCompRating:rating) as PG_RATING,
trim (src:d:overallObjRating:rating) as BG_RATING, 
row_number() over (partition by formdata_id order by to_date(dbt_valid_from) desc,FILE_NAME desc) as RK 
from {{ ref('stg_ye_people_business_goals') }}
 where dbt_valid_to is null
)
where RK=1
{% endsnapshot %}
