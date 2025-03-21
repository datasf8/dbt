{{
    config(
        materialized="view",
        transient=false,
        post_hook ="CALL HRDP_ADM_{{ env_var('DBT_REGION') }}_DB.DB_SCHEMA_SCH.DGRH_ROLE_SP();
                    DROP VIEW {{ this }};"
    )
}}
select *
from {{ ref("dgrh_role_mapping_seed") }}
