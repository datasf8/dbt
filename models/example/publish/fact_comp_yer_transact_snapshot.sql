{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call masking_policy_apply_sp('{{ env_var('DBT_PUB_DB') }}','CMP_PUB_SCH','FACT_COMP_YER_TRANSACT_SNAPSHOT');
        USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ env_var('DBT_PUB_DB') }}','CMP_PUB_SCH','FACT_COMP_YER_TRANSACT_SNAPSHOT');"
    )
}}
SELECT 
*
FROM 
{{ ref("fact_comp_yer_transact") }}
