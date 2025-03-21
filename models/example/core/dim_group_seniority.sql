{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    group_seniority as (
        select
            range_code as range_code,
            range as range,
            case
                when range_code = 9 then 'New Comers Only' else 'Without New Comers'
            end as in_nc
        from {{ ref("dim_range") }}
        where range_type = 'Group'

    ),
    surrogate_key as (
        select p.*, hash(range_code) as sk_range_code from group_seniority p
    )
select
    sk_range_code as dgrs_pk_dgrs,
    range_code as dgrs_range_code_dgrs,
    range as dgrs_range_dgrs,
    in_nc as dgrs_in_nc_dgrs
from surrogate_key
union
select '-1','-1',Null,Null
