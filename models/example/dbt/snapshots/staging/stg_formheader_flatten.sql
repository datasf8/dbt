{% snapshot stg_formheader_flatten %}
    {{

        config(
          unique_key="FORMDATAID",
          strategy='check',
          check_cols='all',
        )

    }}
  
   
select
DATEADD(MS, replace(split_part(split_part(value:"creationDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS creationDate,
Trim(VALUE:"currentStep"::STRING) AS currentStep,
DATEADD(MS, replace(split_part(split_part(value:"dateAssigned", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS dateAssigned,
Trim(VALUE:"formDataId"::STRING) AS formDataId,
Trim(VALUE:"formDataStatus"::STRING) AS formDataStatus,
DATEADD(MS, replace(split_part(split_part(value:"formLastModifiedDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS formLastModifiedDate,
Trim(VALUE:"formOriginator"::STRING) AS formOriginator,
DATEADD(MS, replace(split_part(split_part(value:"formReviewDueDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS formReviewDueDate,
DATEADD(MS, replace(split_part(split_part(value:"formReviewEndDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS formReviewEndDate,
DATEADD(MS, replace(split_part(split_part(value:"formReviewStartDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS formReviewStartDate,
Trim(VALUE:"formSubjectId"::STRING) AS formSubjectId,
Trim(VALUE:"formTemplateId"::STRING) AS formTemplateId,
Trim(VALUE:"formTemplateType"::STRING) AS formTemplateType,
Trim(VALUE:"formTitle"::STRING) AS formTitle,
Trim(VALUE:"isRated"::STRING) AS isRated,
Trim(VALUE:"rating"::STRING) AS rating,
Trim(VALUE:"sender"::STRING) AS sender,
DATEADD(MS, replace(split_part(split_part(value:"stepDueDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS stepDueDate
from {{ ref('stg_formheader') }}
,lateral flatten ( input => src:d:results, OUTER => TRUE)
where dbt_valid_to is null and formdatastatus <> 4
qualify
            row_number() over (
                partition by formSubjectId,formTemplateId
                order by creationDate desc, FORMLASTMODIFIEDDATE desc
            )
            = 1
{% endsnapshot %}
