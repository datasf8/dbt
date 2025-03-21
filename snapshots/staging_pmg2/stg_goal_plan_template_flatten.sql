{% snapshot stg_goal_plan_template_flatten %}
    {{

        config(
          unique_key= "ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

Select
Trim(VALUE:"id"::STRING) AS id,
Trim(VALUE:"mobileFields"::STRING) AS mobileFields,
Trim(VALUE:"parentPlanId"::STRING) AS parentPlanId,
to_date(DATEADD(MS, replace(split_part(value:"dueDate", '(', 2), ')/', ''), '1970-01-01')) AS dueDate,
Trim(VALUE:"defaultTemplate"::STRING) AS defaultTemplate,
Trim(VALUE:"displayOrder"::STRING) AS displayOrder,
Trim(VALUE:"description"::STRING) AS description,
Trim(VALUE:"useTextForPrivacy"::STRING) AS useTextForPrivacy,
Trim(VALUE:"fieldOrder"::STRING) AS fieldOrder,
Trim(VALUE:"name"::STRING) AS name,
Trim(VALUE:"percentageValueOver100"::STRING) AS percentageValueOver100,
to_date(DATEADD(MS, replace(split_part(value:"startDate", '(', 2), ')/', ''), '1970-01-01')) AS startDate
from  {{ ref('stg_goal_plan_template') }}
,lateral flatten (input => src:d:results, OUTER => TRUE)
where dbt_valid_to is null
qualify
    row_number() over (
        partition by id order by to_date(startDate) desc,FILE_NAME desc
    )
    = 1
{% endsnapshot %}
