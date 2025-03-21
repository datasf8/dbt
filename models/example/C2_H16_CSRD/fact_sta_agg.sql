{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}

with
    fact_csrs_agg_cte as (
        select

            business_unit_code,
            business_unit_name_en,
            business_unit_type_code,
            business_unit_type_name_en,
            company_code,
            company_name_en,
            country_code,
            country_name_en,
            geographic_zone_code,
            geographic_zone_name_en,
            measure.csrd_measure_id,
            measure.csrd_calculation_date as date_id,
            sum(measure.csrd_value) as csrd_value_amount
        from
            {{ ref('csrd_headcount_details') }} headcount
        join
            {{ ref('csrd_sta_employee_by_measure') }} measure
            on headcount.csrd_hd_id = measure.csrd_hd_id

        group by
            business_unit_code,
            business_unit_name_en,
            business_unit_type_code,
            business_unit_type_name_en,
            company_code,
            company_name_en,
            country_code,
            country_name_en,
            geographic_zone_code,
            geographic_zone_name_en,
            measure.csrd_measure_id,
            measure.csrd_calculation_date
    )
select 
* from fact_csrs_agg_cte