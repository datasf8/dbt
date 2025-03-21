{{ config(schema="cmn_pub_sch") }}
select headcount_type_code, collate(headcount_type_name, 'en-ci') as headcount_type_name
from {{ ref("dim_headcount_type_v1") }}
