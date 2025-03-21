{{
    config(
        materialized="incremental",
        unique_key="CSRD_EBMH_CALCULATION_DATE",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook="
        ALTER TABLE {{ this }} SET DATA_RETENTION_TIME_IN_DAYS=90;",
    )
}}
 
with
    date_cte as (

        select date_month_code, date_month_start_date, date_month_end_date
        from {{ ref("date_month_referential") }}
        where date_month_code >= 202401
    ) 

select 
    hash(CSRD_HD_ID, csrd_measure_id, csrd_calculation_date) as csrd_ebmh_id,
    csrd_calculation_date as csrd_ebmh_calculation_date,  
    csrd_measure_id,
    hash(CSRD_HD_ID, csrd_calculation_date)  as csrd_hdh_id, 
    csrd_value
 from {{ ref('csrd_employee_by_measure') }}
{% if is_incremental() %}

    where
        csrd_calculation_date
        >= (select coalesce(max(CSRD_EBMH_CALCULATION_DATE), '202401') from {{ this }})

{% endif %}
 