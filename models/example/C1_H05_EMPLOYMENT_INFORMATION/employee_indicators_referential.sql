{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    hash(
        employee_indicators_referential_start_date,
        employee_indicators_code,
        headcount_type_code
    ) as employee_indicators_referential_id,
    to_date(
        employee_indicators_referential_start_date::string, 'yyyymmdd'
    ) as employee_indicators_referential_start_date,
    to_date(
        employee_indicators_referential_end_date::string, 'yyyymmdd'
    ) as employee_indicators_referential_end_date,
    employee_indicators_code,
    employee_indicators_name,
    employee_indicators_sort,
    employee_indicators_group_code,
    employee_indicators_group_name,
    employee_indicators_group_sort,
    employee_indicators_subgroup_code,
    employee_indicators_subgroup_name,
    employee_indicators_subgroup_sort,
    headcount_type_code
from {{ ref("employee_indicators_referential_seed") }}
