{{
    config(
        materialized="table",
        transient=false,
    )
}}
select *
from {{ ref("document_status_seed") }}