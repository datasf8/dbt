{{ config(schema="ack_pub_sch") }}
select {{ dbt_utils.star(ref("fact_document_acknowledgment")) }},
from {{ ref("fact_document_acknowledgment") }}
