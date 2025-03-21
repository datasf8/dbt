{% snapshot stg_cust_acknowledgement_v2 %}
    {{
        config(
            unique_key="SRC",
            strategy="check",
            check_cols="all",
            target_schema="SDDS_STG_SCH",
        )
    }}

    select src, max(file_name) as file_name
    from {{ source("landing_tables_CMN", "CUST_ACKNOWLEDGEMENT_V2") }}
    group by src
{% endsnapshot %}
