{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook="
        ALTER TABLE {{ this }} SET DATA_RETENTION_TIME_IN_DAYS=90;",
    )
}}

with
    fact_csrs_agg_cte_histo as (
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
            measure.csrd_ebmh_calculation_date as date_id,
            sum(measure.csrd_value) as csrd_value_amount
        from
            {{ ref('csrd_headcount_details_histo') }} headcount
        join
            {{ ref('csrd_employee_by_measure_histo') }} measure
            on headcount.csrd_hdh_id = measure.csrd_hdh_id

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
            measure.csrd_ebmh_calculation_date
    )
select 
* from fact_csrs_agg_cte_histo 