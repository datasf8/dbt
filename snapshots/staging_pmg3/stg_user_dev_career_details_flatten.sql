{% snapshot stg_user_dev_career_details_flatten %}
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
Trim(VALUE:"carAspiration"::STRING) as car_Aspiration,
row_number() over (partition by USER_ID order by Last_Modified_Date desc,FILE_NAME desc) as RK
from {{ ref('stg_user_dev_career_details') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null
) where RK=1
{% endsnapshot %}
