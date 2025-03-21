
{% snapshot stg_background_custcareerasp_flatten %}
    {{
        config(
          unique_key= "backgroundElementId||'-'||userId",
          strategy='check',
          check_cols='all',
          target_schema= "SDDS_STG_SCH",
          invalidate_hard_deletes=true
        )
    }}

select 
Trim(VALUE:"backgroundElementId"::STRING) AS backgroundElementId,
Trim(VALUE:"userId"::STRING) AS userId,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDate,
Trim(VALUE:"bgOrderPos"::STRING) AS bgOrderPos,
to_date(DATEADD(MS, replace(split_part(value:"lastModified", '(', 2), ')/', ''), '1970-01-01')) AS lastModified,
Trim(VALUE:"carAspiration"::STRING) AS carAspiration
from {{ ref('stg_background_custcareerasp') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by backgroundElementId, userId
                order by lastModifiedDate desc
            )
            = 1
{% endsnapshot %}
