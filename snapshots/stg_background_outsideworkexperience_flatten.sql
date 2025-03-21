
{% snapshot stg_background_outsideworkexperience_flatten %}
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
Trim(VALUE:"country"::STRING) AS country,
to_date(DATEADD(MS, replace(split_part(value:"endDate", '(', 2), ')/', ''), '1970-01-01')) AS endDate,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDate,
Trim(VALUE:"bgOrderPos"::STRING) AS bgOrderPos,
Trim(VALUE:"company"::STRING) AS company,
Trim(VALUE:"job"::STRING) AS job,
to_date(DATEADD(MS, replace(split_part(value:"startDate", '(', 2), ')/', ''), '1970-01-01')) AS startDate
from {{ ref('stg_background_outsideworkexperience') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by backgroundElementId, userId
                order by lastModifiedDate desc
            )
            = 1
{% endsnapshot %}
