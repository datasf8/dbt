{% snapshot stg_country %}
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
FROM  {{ source('landing_tables_CMN', 'COUNTRY') }}
group by src
{% endsnapshot %}