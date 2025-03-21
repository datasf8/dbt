{% snapshot stg_ye_signature_comments %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
src,max(FILE_NAME) as FILE_NAME
FROM  {{ source('landing_tables_PMG', 'YE_SIGNATURE_COMMENTS') }}
group by src
{% endsnapshot %}
