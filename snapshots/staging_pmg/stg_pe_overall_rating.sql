 {% snapshot stg_pe_overall_rating %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
 src,max(FILE_NAME) as FILE_NAME
FROM  {{ source('landing_tables_PMG', 'PE_OVERALL_RATING') }}
group by src
{% endsnapshot %}
