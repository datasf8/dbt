{% snapshot stg_user_goal_details_flatten %}
    {{

        config(
          unique_key= "GOAL_ID||'-'||GOAL_TEMPLATE_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

Select
Trim(VALUE:"userId"::STRING) as USER_ID,
Trim(VALUE:"id"::INT) as GOAL_ID,
TRIM(VALUE:"name"::string) as GOAL_Name,
TRIM(VALUE:"metric"::string) as KPI_EXPECTED_RESULTS,
TRIM(VALUE:"category"::string) as CATEGORY,
TRIM(VALUE:"state" ::string) as STATUS,
to_date(DATEADD(MS, replace(split_part(value:"due",'(',2),')/',''), '1970-01-01')) as DUE_DATE,
to_date(DATEADD(MS, replace(split_part(value:"lastModified",'(',2),')/',''), '1970-01-01')) as LAST_MODIFIED_DATE,
TRIM(VALUE:"flag"::string) as GOAL_CHARACTERISTIC,
split_part(VALUE:"__metadata":"type",'_', -1) as goal_template_id,
Trim(VALUE:"numbering"::STRING) AS numbering,
Trim(VALUE:"modifier"::STRING) AS modifier,
Trim(VALUE:"type"::STRING) AS type,
Trim(VALUE:"currentOwner"::STRING) AS currentOwner,
Trim(VALUE:"guid"::STRING) AS guid,
Trim(VALUE:"stateLabel"::STRING) AS stateLabel,
to_date(DATEADD(MS, replace(split_part(value:"dueDateInUTC", '(', 2), ')/', ''), '1970-01-01')) AS dueDateInUTC
from  {{ ref('stg_user_goal_details') }}
,lateral flatten (input => src:d:results, OUTER => TRUE)
where dbt_valid_to is null 
 qualify
    row_number() over (
        partition by GOAL_ID, goal_template_id order by to_date(LAST_MODIFIED_DATE) desc,FILE_NAME desc
    )
    = 1
{% endsnapshot %}
