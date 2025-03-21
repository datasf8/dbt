{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    fopaycomponentgroup as (
        select
            iff(
                enddate in ('9999-12-31'),
                lead(startdate - 1, 1, {d '9999-12-31'}) over (
                    partition by externalcode order by startdate
                ),
                enddate
            ) as c_enddate,
            *
        from {{ ref("stg_fo_paycomponent_group_flatten") }}
        where dbt_valid_to is null

    )
select
    hash(externalcode, startdate) as pay_component_group_id,
    externalcode as pay_component_group_code,
    startdate as pay_component_group_start_date,
    c_enddate as pay_component_group_end_date,
    name as pay_component_group_name,
    useforcomparatiocalc as used_for_penetration_flag,
    useforrangepenetration as used_for_compa_ratio_flag,
    status as pay_component_group_status
from fopaycomponentgroup
