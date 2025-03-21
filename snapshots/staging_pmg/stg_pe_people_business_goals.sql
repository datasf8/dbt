{% snapshot stg_pe_people_business_goals %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
 src,max(FILE_NAME) as FILE_NAME
FROM  {{ source('landing_tables_PMG', 'PE_PEOPLE_BUSINESS_GOALS') }}
group by src
{% endsnapshot %}
