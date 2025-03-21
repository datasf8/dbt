
{% snapshot stg_cust_acknowledgement_flatten %}
    {{
        config(
          unique_key= "externalCode||'-'||cust_AcknowledgementList_externalCode",
          strategy='check',
          check_cols='all',
          target_schema= "SDDS_STG_SCH",
          invalidate_hard_deletes=true
        )
    }}

select 
Trim(VALUE:"externalCode"::STRING) AS externalCode,
Trim(VALUE:"cust_AcknowledgementList_externalCode"::STRING) AS cust_AcknowledgementList_externalCode,
DATEADD(MS, replace(split_part(split_part(value:"cust_validationDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS cust_validationDate,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDateTime,
DATEADD(MS, replace(split_part(split_part(value:"cust_dateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS cust_dateTime,
Trim(VALUE:"lastModifiedBy"::STRING) AS lastModifiedBy,
DATEADD(MS, replace(split_part(split_part(value:"createdDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS createdDateTime,
Trim(VALUE:"mdfSystemRecordStatus"::STRING) AS mdfSystemRecordStatus,
Trim(VALUE:"cust_paperSignature"::STRING) AS cust_paperSignature,
Trim(VALUE:"cust_versionID"::STRING) AS cust_versionID,
DATEADD(MS, replace(split_part(split_part(value:"cust_consultationDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS cust_consultationDate,
Trim(VALUE:"createdBy"::STRING) AS createdBy,
Trim(VALUE:"externalName"::STRING) AS externalName,
Trim(VALUE:"cust_status"::STRING) AS cust_status,
Trim(VALUE:"cust_comment"::STRING) AS cust_comment,
Trim(VALUE:"cust_acknowledgementID"::STRING) AS cust_acknowledgementID
from {{ ref('stg_cust_acknowledgement') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by externalCode, cust_AcknowledgementList_externalCode
                order by lastModifiedDateTime desc
            )
            = 1
{% endsnapshot %}
