{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','DOCUMENT_ACKNOWLEDGMENT');"
    )
}}
select
    externalcode as acknowledgment_id,
    dbt_valid_from as acknowledgment_start_date,
    nvl(dbt_valid_to, '9999-12-31') as acknowledgment_end_date,
    cust_acknowledgementlist_externalcode as user_id,
    cust_model_id as document_id,
    cust_consultationdate as consultation_date,
    cust_validationdate as validation_date,
    cust_status as acknowledgment_status_id
from {{ ref("stg_cust_acknowledgement_v2_flatten") }}
