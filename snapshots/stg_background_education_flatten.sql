
{% snapshot stg_background_education_flatten %}
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
Trim(VALUE:"comments"::STRING) AS comments,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDate", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDate,
Trim(VALUE:"year"::STRING) AS year,
Trim(VALUE:"degree"::STRING) AS degree,
Trim(VALUE:"internationalProgram"::STRING) AS internationalProgram,
Trim(VALUE:"degree_type"::STRING) AS degree_type,
Trim(VALUE:"school"::STRING) AS school,
Trim(VALUE:"bgOrderPos"::STRING) AS bgOrderPos,
Trim(VALUE:"school_new"::STRING) AS school_new
from {{ ref('stg_background_education') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by backgroundElementId, userId
                order by lastModifiedDate desc
            )
            = 1
{% endsnapshot %}
