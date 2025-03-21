{{
    config(
        materialized="table",
        transient=false,
    )
}}
select hash(*) as headcount_type_id,*
from {{ ref("headcount_type_seed") }}