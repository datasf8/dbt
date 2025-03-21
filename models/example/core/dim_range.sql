{{ config(materialized="table", transient=false) }}


with
    d_range as (
        select distinct range_code, range_type, range, range_inf, range_sup
        from {{ ref("range") }}
    )
select range_code, range_type, range, range_inf, range_sup
from d_range
union
select '-1',NUll,Null,Null,NUll
