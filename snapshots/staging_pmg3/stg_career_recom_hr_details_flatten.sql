{% snapshot stg_career_recom_hr_details_flatten %}
    {{

        config(
          unique_key= "USER_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

select * from (
select distinct
Trim(VALUE:"userId"::STRING) as USER_ID,
to_date(DATEADD(MS, replace(split_part(value:"lastModified",'(',2),')/',''), '1970-01-01')) as Last_Modified_Date,
Trim(VALUE:"carRecommendationsHR"::STRING) as car_Recommendations_HR,
Trim(VALUE:"writtenBy"::STRING) as WRITTEN_BY,
row_number() over (partition by USER_ID order by Last_Modified_Date desc,FILE_NAME desc) as RK
from {{ ref('stg_career_recom_hr_details') }}
,lateral flatten (input => src:d:results, OUTER => TRUE)where dbt_valid_to is null
) where RK=1
{% endsnapshot %}
