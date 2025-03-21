 {% snapshot stg_formobjcompsummarysection %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
 src,max(FILE_NAME) as FILE_NAME
    FROM  {{ source('landing_tables_SDDS', 'FORMOBJCOMPSUMMARYSECTION') }}
group by src
{% endsnapshot %}
