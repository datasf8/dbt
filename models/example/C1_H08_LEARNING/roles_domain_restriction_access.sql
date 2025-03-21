{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select role_id as role_code, dmn_restriction_id as domain_restriction_code
from {{ ref("stg_pa_role_wf_entity_fct_access") }}
where workflow_entity_fct_id = 'View Class.Class.View' and  dbt_valid_to is null
