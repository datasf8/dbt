{{ config(schema="cmn_pub_sch") }}
select
    user_id as employee_id,
    employee_profile_sk as employee_skey,
    user_full_name as employee_full_name,
    direct_manager as direct_manager_id,
    direct_manager_full_name as direct_manager_full_name,
    manager_id as manager_id,
    manager_full_name as manager_full_name,
    manager_full_name as manager_full_name_filter,
    level as level
from {{ ref("dim_employee_profile_manager") }}
