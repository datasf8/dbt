{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_csrd_employee_group_mapping")) }}
from {{ ref("dim_csrd_employee_group_mapping") }}
