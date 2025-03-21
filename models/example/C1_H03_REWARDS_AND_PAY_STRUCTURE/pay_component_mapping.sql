{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    pc.pay_component_id,
    flx_externalcode as pay_component_code,
    pcg.pay_component_group_id,
    group_externalcode as pay_component_group_code
from {{ ref("stg_fo_pay_component_group_flx_flatten") }} output1
left join
    {{ ref("pay_component") }} pc on pc.pay_component_code = output1.flx_externalcode
left join
    {{ ref("pay_component_group") }} pcg
    on pcg.pay_component_group_code = output1.group_externalcode
where dbt_valid_to is null and enddate = '9999-12-31'
qualify
    row_number() over (
        partition by output1.flx_externalcode, output1.group_externalcode
        order by
            lastmodifieddatetime desc,
            pc.pay_component_start_date desc,
            pcg.pay_component_group_start_date desc
    )
    = 1