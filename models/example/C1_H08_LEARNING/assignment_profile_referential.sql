{{
    config(
        materialized="table",
        unique_key="1",
        transient=false,
    )
}}
select *
from {{ ref("dim_param_assignment_profile_referential") }}