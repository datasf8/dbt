{{ config(schema="ack_pub_sch") }}
select
    {{ dbt_utils.star(ref("dim_document")) }},
    document_name_en || ' V' || document_version as document_name_version
from {{ ref("dim_document") }}
