{% snapshot stg_formheader %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
src,max(FILE_NAME) as FILE_NAME
FROM {{ source('landing_tables_SDDS', 'FORMHEADER') }}
group by src
{% endsnapshot %}
