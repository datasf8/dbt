{{
    config(
        materialized="table",
        transient=false,
        on_schema_change="sync_all_columns",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call masking_policy_apply_sp('{{ env_var('DBT_PUB_DB') }}','CMP_PUB_SCH','FACT_VARIABLE_PAY_SNAPSHOT');
        USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ env_var('DBT_PUB_DB') }}','CMP_PUB_SCH','FACT_VARIABLE_PAY_SNAPSHOT');"
    )
}}

SELECT 
*
FROM 
{{ ref("fact_variable_pay") }} order by  ftvp_user_id_ftvp