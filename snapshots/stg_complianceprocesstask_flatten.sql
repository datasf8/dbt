
{% snapshot stg_complianceprocesstask_flatten %}
    {{
        config(
          unique_key= "TaskId",
          strategy='check',
          check_cols='all',
          target_schema= "SDDS_STG_SCH"
        )
    }}

select 
Trim(VALUE:"taskId"::STRING) AS taskId,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDateTime,
to_date(DATEADD(MS, replace(split_part(value:"endDate", '(', 2), ')/', ''), '1970-01-01')) AS endDate,
to_date(DATEADD(MS, replace(split_part(value:"dueDate", '(', 2), ')/', ''), '1970-01-01')) AS dueDate,
DATEADD(MS, replace(split_part(split_part(value:"createdDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS createdDateTime,
Trim(VALUE:"type"::STRING) AS type,
Trim(VALUE:"subjectUser"::STRING) AS subjectUser,
Trim(VALUE:"process"::STRING) AS process,
Trim(VALUE:"lastModifiedBy"::STRING) AS lastModifiedBy,
Trim(VALUE:"mdfSystemRecordStatus"::STRING) AS mdfSystemRecordStatus,
Trim(VALUE:"createdBy"::STRING) AS createdBy,
Trim(VALUE:"category"::STRING) AS category,
Trim(VALUE:"completedBy"::STRING) AS completedBy,
to_date(DATEADD(MS, replace(split_part(value:"startDate", '(', 2), ')/', ''), '1970-01-01')) AS startDate,
Trim(VALUE:"status"::STRING) AS status
from {{ ref('stg_complianceprocesstask') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by taskId
                order by lastModifiedDateTime desc
            )
            = 1
{% endsnapshot %}
