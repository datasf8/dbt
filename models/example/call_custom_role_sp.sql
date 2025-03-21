{{
    config(
        materialized="view",
        transient=false,
        post_hook ="CALL HRDP_ADM_{{ env_var('DBT_REGION') }}_DB.DB_SCHEMA_SCH.CUSTOM_ROLE_SP();
                    DROP VIEW {{ this }};"
    )
}}
select *
from {{ ref("custom_role_mapping_seed") }}
