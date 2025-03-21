{{
    config(
        materialized="table",
        transient=false,
    )
}}
select hash(range_type, range_start) as range_id, *
from {{ ref("ranges_seed") }}
