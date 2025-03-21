{% snapshot stg_background_languages %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          target_schema= "SDDS_STG_SCH"
          
        )

    }}

select
  src,max(FILE_NAME) AS FILE_NAME
FROM  {{ source('landing_tables_CMN', 'BACKGROUND_LANGUAGES') }}
group by src
{% endsnapshot %}