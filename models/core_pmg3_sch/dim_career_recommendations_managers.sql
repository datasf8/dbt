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
    recommendations as (
        select distinct
            user_id,
            car_recommendations,
            last_modified_date,
            ddep_employee_profile_sk_ddep
        from
            (
                select *
                from {{ ref("stg_career_recom_details_flatten") }}
                where dbt_valid_to is null
            ) crdf
        left outer join
            (select * from {{ ref("dim_employee_profile") }}) dep
            on crdf.user_id = dep.ddep_employee_id_ddep
    )

select
    ddep_employee_profile_sk_ddep as dcrm_employee_profile_key_ddep,
    car_recommendations as dcrm_career_recommendations_dcrm,
    last_modified_date as dcrm_last_modified_date_dcrm
from recommendations
