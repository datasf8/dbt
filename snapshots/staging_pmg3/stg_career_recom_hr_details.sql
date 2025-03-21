{% snapshot stg_career_recom_hr_details %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
  src,max(FILE_NAME) AS FILE_NAME
FROM  {{ source('landing_tables_PMG', 'CAREER_RECOM_HR_DETAILS') }}
group by src
{% endsnapshot %}
