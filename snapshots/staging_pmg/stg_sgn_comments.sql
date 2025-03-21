{% snapshot stg_sgn_comments %}
    {{

        config(
          unique_key="SRC",
          strategy='check',
          check_cols='all',
          
        )

    }}

select
src,max(FILE_NAME) as FILE_NAME
FROM  {{ source('landing_tables_PMG', 'SGN_COMMENTS') }}
group by src
{% endsnapshot %}
