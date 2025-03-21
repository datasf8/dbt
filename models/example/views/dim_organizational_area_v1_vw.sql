{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_organizational_area_v1")) }}
from {{ ref("dim_organizational_area_v1") }}
