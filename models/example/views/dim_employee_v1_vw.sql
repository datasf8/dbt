{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_employee_v1")) }}, ' ' as fakecolumn
from {{ ref("dim_employee_v1") }}
