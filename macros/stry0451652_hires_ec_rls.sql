{% macro stry0451652_hires_ec_rls() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        -- Delete and Insert of tables with Policy into RLS Setup Table
        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0451652';
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_HIRES_EC','USER_ID',current_timestamp(),'STRY0451652');
        
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');
        --call cmn_core_sch.rls_policy_apply_all_sp('HRDP_CORE_{{ env_var('DBT_REGION') }}_DB','CMN_CORE_SCH');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
