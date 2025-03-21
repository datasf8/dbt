{% snapshot stg_cust_employee_group %}
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
FROM  {{ source('landing_tables_CMN', 'CUST_EMPLOYEE_GROUP') }}
group by src
{% endsnapshot %}