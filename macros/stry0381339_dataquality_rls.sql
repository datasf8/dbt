{% macro stry0381339_dataquality_rls() %}
 
    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0381339';
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','LRN_PUB_SCH','FACT_DQ_INACCURATE_HISTORY','USER_ID',current_timestamp(),'STRY0381339');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','LRN_PUB_SCH','FACT_DQ_MISSING_COMPLETION','USER_ID',current_timestamp(),'STRY0381339');
        --call rls_policy_apply_sp('HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','LRN_PUB_SCH','FACT_DQ_INACCURATE_HISTORY');
        call cmn_core_sch.rls_policy_apply_all_sp('HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','LRN_PUB_SCH');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}