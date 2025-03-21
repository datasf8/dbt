{{ config(materialized="table", unique_key="SFROLEID", transient=false) }}



select distinct

    trim(value:"roleId"::string) as role_id, collate(trim(value:"roleName"::string),'en-ci') as role_name

from
    {{ ref("stg_role_details") }},
    lateral flatten(input => src:d:results, outer => true)
