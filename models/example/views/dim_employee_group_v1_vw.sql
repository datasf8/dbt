{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_employee_group_v1")) }}
from {{ ref("dim_employee_group_v1") }}
