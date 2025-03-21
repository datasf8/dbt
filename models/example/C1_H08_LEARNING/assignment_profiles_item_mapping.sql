{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}


select
    cpnt_id as item_code,
    ap_id as assignment_profile_id,
    dbt_valid_from as assignment_profiles_item_mapping_data_start_date,
    dbt_valid_to as assignment_profiles_item_mapping_data_end_date,
    status as assingment_profiles_item_mapping_status

from {{ ref("stg_pa_cpnt_ap") }}

