{{ config(schema="cmn_pub_sch") }}
select
    {{ dbt_utils.star(ref("dim_employee_manager_v1")) }},
    manager_name manager_name_filter
from {{ ref("dim_employee_manager_v1") }}
