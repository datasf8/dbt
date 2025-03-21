{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change='sync_all_columns',
        cluster_by=['user_id','headcount_type_code'],
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_HIRES_EC');"
    )
}}
select {{ dbt_utils.star(ref("fact_hires")) }}
from {{ ref("fact_hires") }}
