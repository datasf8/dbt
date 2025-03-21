{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("fact_csrd_employee_by_measure_histo")) }}
from {{ ref("fact_csrd_employee_by_measure_histo") }}
