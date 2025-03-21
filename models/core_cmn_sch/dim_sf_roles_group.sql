{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}

with
    sf_roles_group as (
        select sfroleid, role_name, grtgroup, grtgroupname, tgtgroup, tgtgroupname
        from
            (
                select distinct sfroleid, grtgroup, grtgroupname, tgtgroup, tgtgroupname
                from {{ ref("stg_sf_roles_group_flatten") }}
            ) srgf
        left outer join
            {{ ref("stg_role_details_flatten") }} srdf on srgf.sfroleid = srdf.role_id
    )
select *
from sf_roles_group
