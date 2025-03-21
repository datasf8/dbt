{% macro stry0366394_im_rls() %}
 
    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0366394';
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_INTERNAL_MOBILITY','USER_ID',current_timestamp(),'STRY0366394');
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
