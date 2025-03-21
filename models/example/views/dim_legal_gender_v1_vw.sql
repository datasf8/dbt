{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_legal_gender_v1")) }}
from {{ ref("dim_legal_gender_v1") }}
