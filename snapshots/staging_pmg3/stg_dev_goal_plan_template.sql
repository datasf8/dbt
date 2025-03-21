{% snapshot stg_dev_goal_plan_template %}
    {{

        config(
          unique_key='SRC',
          strategy='check',
          check_cols='all',
          
        )

    }}

select
  src,max(FILE_NAME) AS FILE_NAME
FROM  {{ source('landing_tables_PMG', 'DEV_GOAL_PLAN_TEMPLATE') }}
group by src
{% endsnapshot %}
