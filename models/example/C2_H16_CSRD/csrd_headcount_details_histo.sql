{{
    config(
        materialized="incremental",
        unique_key="csrd_hdh_calculation_date",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook="
        ALTER TABLE {{ this }} SET DATA_RETENTION_TIME_IN_DAYS=90;
        update {{ this }} as t1  set t1.flag_purged = iff(t2.user_id is null, 1, 0)  from {{ ref('employee_profile_directory') }} as t2 WHERE t1.user_id = t2.user_id (+);                  
        USE DATABASE {{ env_var('DBT_CORE_DB') }};
        use schema CMN_CORE_SCH;         
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','CSRD_HEADCOUNT_DETAILS_HISTO');",
    )
}}

with
    date_cte as (

        select date_month_code, date_month_start_date, date_month_end_date
        from {{ ref("date_month_referential") }}
        where date_month_code >= 202401
    )

select
    hash(csrd_hd_id, date_ref.date_month_code) as csrd_hdh_id,
    date_ref.date_month_code as csrd_hdh_calculation_date,
    csrd_hd_id,
    csrd_hd_start_date,
    csrd_hd_end_date,
    personal_id,
    user_id,
    legal_first_name,
    legal_last_name,
    date_of_birth,
    legal_gender_code,
    legal_gender_name_en,
    employee_status_code,
    employee_status_name_en,
    local_contract_type_code,
    local_contract_type_name_en,
    employee_group_code,
    employee_group_name_en,
    employee_subgroup_code,
    employee_subgroup_name_en,
    cost_center_code,
    cost_center_name_en,
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
    disability_status_code,
    disability_status_name_en,
    key_position_type_code,
    key_position_type_name_en,
    termination_date,
    flag_purged

from {{ ref("csrd_headcount_details") }} headcount, date_cte date_ref

{% if is_incremental() %}

    where
        date_ref.date_month_code
        >= (select coalesce(max(csrd_hdh_calculation_date), '202401') from {{ this }})

{% endif %}
