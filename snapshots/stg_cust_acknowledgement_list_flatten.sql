
{% snapshot stg_cust_acknowledgement_list_flatten %}
    {{
        config(
          unique_key= "externalCode",
          strategy='check',
          check_cols='all',
          target_schema= "SDDS_STG_SCH",
          invalidate_hard_deletes=true
        )
    }}

select 
Trim(VALUE:"externalCode"::STRING) AS externalCode,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDateTime,
Trim(VALUE:"lastModifiedBy"::STRING) AS lastModifiedBy,
DATEADD(MS, replace(split_part(split_part(value:"createdDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS createdDateTime,
Trim(VALUE:"mdfSystemRecordStatus"::STRING) AS mdfSystemRecordStatus,
Trim(VALUE:"createdBy"::STRING) AS createdBy
from {{ ref('stg_cust_acknowledgement_list') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by externalCode
                order by lastModifiedDateTime desc
            )
            = 1
{% endsnapshot %}
