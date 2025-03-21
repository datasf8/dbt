{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        cluster_by=["CSRD_HDH_CALCULATION_DATE"],        
        incremental_strategy="delete+insert",
        post_hook="
        ALTER TABLE {{ this }} SET DATA_RETENTION_TIME_IN_DAYS=90;
        USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','FACT_CSRD_HEADCOUNT_DETAILS_HISTO');"    
    )
  
}}


select *, 'CSR' as headcount_type_code
from {{ ref("csrd_headcount_details_histo") }}
union all

select
    csrd_hd_id as csrd_hdh_id,
    to_number(to_varchar(current_timestamp, 'yyyyMM')) as csrd_hdh_calculation_date,
    *,
    'STA' as headcount_type_code
from {{ ref("csrd_headcount_details") }}
