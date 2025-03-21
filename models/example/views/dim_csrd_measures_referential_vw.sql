{{ config(schema="cmn_pub_sch") }}
select {{ dbt_utils.star(ref("dim_csrd_measures_referential")) }}
from {{ ref("dim_csrd_measures_referential") }}
