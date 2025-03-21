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
    aspirations as (
        select distinct
            user_id,
            car_aspiration,
            last_modified_date,
            ddep_employee_profile_sk_ddep
        from
            (
                select *
                from {{ ref("stg_user_dev_career_details_flatten") }}
                where dbt_valid_to is null
            ) udcdf
        left outer join
            (select * from {{ ref("dim_employee_profile") }}) dep
            on udcdf.user_id = dep.ddep_employee_id_ddep
    )

select
    ddep_employee_profile_sk_ddep as dcra_employee_profile_key_ddep,
    car_aspiration as dcra_career_aspirations_dcra,
    last_modified_date as dcra_last_modified_date_dcra
from aspirations
