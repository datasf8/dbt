{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
select
    hash(externalcode,effectivestartdate) as cost_center_group_id,
    externalcode as cost_center_group_code,
    effectivestartdate as cost_center_group_start_date,
    mdfsystemeffectiveenddate as cost_center_group_end_date,
    externalname_defaultvalue as cost_center_group_name_en,
    externalname_fr_fr as cost_center_group_name_fr,
    mdfsystemstatus as cost_center_group_status
from {{ ref("stg_cust_groupcostcenter_flatten") }}
where dbt_valid_to is null
