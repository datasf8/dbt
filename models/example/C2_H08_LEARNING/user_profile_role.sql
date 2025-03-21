{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    user_name as user_id,
    role_id as role_code
from {{ ref('stg_pa_user_prfl_role') }}
where dbt_valid_to is null
qualify
            row_number() over (
                partition by user_name, role_id order by role_id desc
            )
            = 1