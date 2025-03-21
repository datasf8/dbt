{{
    config(
        materialized="table",
        unique_key="1",
        transient=false,
    )
}}
select
    to_date(csrd_lctm_start_date::string, 'yyyymmdd') as csrd_lctm_start_date,
    to_date(csrd_lctm_end_date::string, 'yyyymmdd') as csrd_lctm_end_date,
    country_code,
    employee_group_code,
    employee_subgroup_code,
    local_contract_code
from {{ ref("csrd_local_contract_type_mapping_seed") }}
