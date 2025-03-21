{{
    config(
        materialized="table",
        transient=false,
    )
}}

select *
from {{ ref("dim_position_in_range") }}
where dpir_id_position_in_range_dpir <> 6
