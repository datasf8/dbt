{% snapshot stg_form_content %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
  src,max(FILE_NAME) as FILE_NAME
FROM  {{ source('landing_tables_PMG', 'FORM_CONTENT') }}
group by src
{% endsnapshot %}
