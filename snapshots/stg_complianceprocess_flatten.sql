
{% snapshot stg_complianceprocess_flatten %}
    {{
        config(
          unique_key= "processId",
          strategy='check',
          check_cols='all',
          target_schema= "SDDS_STG_SCH"
        )
    }}

select 
Trim(VALUE:"processId"::STRING) AS processId,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDateTime,
Trim(VALUE:"processStatus"::STRING) AS processStatus,
DATEADD(MS, replace(split_part(split_part(value:"createdDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS createdDateTime,
Trim(VALUE:"correctDataTriggered"::STRING) AS correctDataTriggered,
Trim(VALUE:"processType"::STRING) AS processType,
Trim(VALUE:"processInitiatorId"::STRING) AS processInitiatorId,
Trim(VALUE:"complianceMasterId"::STRING) AS complianceMasterId,
Trim(VALUE:"lastModifiedBy"::STRING) AS lastModifiedBy,
Trim(VALUE:"processInitiatorType"::STRING) AS processInitiatorType,
Trim(VALUE:"mdfSystemRecordStatus"::STRING) AS mdfSystemRecordStatus,
Trim(VALUE:"onboardingProcess"::STRING) AS onboardingProcess,
Trim(VALUE:"createdBy"::STRING) AS createdBy,
Trim(VALUE:"user"::STRING) AS user,
to_date(DATEADD(MS, replace(split_part(value:"startDate", '(', 2), ')/', ''), '1970-01-01')) AS startDate
from {{ ref('stg_complianceprocess') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by processId
                order by lastModifiedDateTime desc
            )
            = 1
{% endsnapshot %}
