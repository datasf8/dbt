{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode,startdate) as pay_group_id,
    externalcode as pay_group_code ,
    startdate as pay_group_start_date,
    enddate as pay_group_end_date,
    name_defaultvalue as pay_group_name_en,
    name_fr_fr as pay_group_name_fr,
    status as pay_group_status
from {{ ref("stg_fopaygroup_flatten") }}
where dbt_valid_to is null