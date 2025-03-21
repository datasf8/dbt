{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select distinct tag_value as dprt_tag_value_dprt, role_name as dprt_role_name_dprt
from {{ source("landing_tables_CMN", "PARAM_ROLES_TAGS") }}
