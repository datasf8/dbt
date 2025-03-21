{% snapshot stg_user_goal_details %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
  src,max(FILE_NAME) AS FILE_NAME
FROM  {{ source('landing_tables_PMG', 'USER_GOAL_DETAILS') }}
group by src
{% endsnapshot %}
