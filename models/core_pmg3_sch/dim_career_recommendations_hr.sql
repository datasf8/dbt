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
    recommendations_hr as (
        select distinct
            user_id,
            car_recommendations_hr,
            written_by,
            last_modified_date,
            ddep_employee_profile_sk_ddep
        from
            (
                select *
                from {{ ref("stg_career_recom_hr_details_flatten") }}
                where dbt_valid_to is null
            ) crhdf
        left outer join
            (select * from {{ ref("dim_employee_profile") }}) dep
            on crhdf.user_id = dep.ddep_employee_id_ddep
    )

select
    ddep_employee_profile_sk_ddep as dcrh_employee_profile_key_ddep,
    car_recommendations_hr as dcrh_recommendations_hr_dcrh,
    written_by as dcrh_written_by_dcrh,
    last_modified_date as dcrh_last_modified_date_dcrh
from recommendations_hr
