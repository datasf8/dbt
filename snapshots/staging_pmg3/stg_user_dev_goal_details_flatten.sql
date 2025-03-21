{% snapshot stg_user_dev_goal_details_flatten %}
    {{

        config(
          unique_key= "USER_ID||'-'||Goal_ID||'-'||DEV_GOAL_TEMPLATE_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

select distinct
Trim(VALUE:"userId"::STRING) as USER_ID,
Trim(VALUE:"id"::INT) as Goal_ID,
Trim(VALUE:"name"::STRING) as Goal_Name,
Trim(VALUE:"category"::STRING) as Category,
Trim(VALUE:"state"::STRING) as Goal_Status,
Trim(VALUE:"purposeLabel"::STRING) as Purpose_label,
Trim(VALUE:"purpose"::STRING) as Purpose,
split_part(split_part(file_name,'/',-1),'_',-3) as dev_goal_template_id,
Trim(VALUE:"goalModifier":"empId"::STRING) as Goal_Modifier ,
Trim(VALUE:"goalOwner":"empId"::STRING) as Goal_Owner,
to_date(DATEADD(MS, replace(split_part(value:"lastModified",'(',2),')/',''), '1970-01-01')) as Last_Modified_Date,
to_date(DATEADD(MS, replace(split_part(value:"due",'(',2),')/',''), '1970-01-01')) as DUE_DATE
from {{ ref('stg_user_dev_goal_details') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
    row_number() over (
        partition by USER_ID,GOAL_ID, dev_goal_template_id order by to_date(LAST_MODIFIED_DATE) desc,FILE_NAME desc
    )
    = 1
{% endsnapshot %}
