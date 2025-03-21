{% macro stry0459509_pmgm_rls() %}
 
    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0459509';
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('PMGM','HRDP_CORE_{{ env_var('DBT_REGION') }}_DB','PMG_CORE_SCH','YE_FORM_ATTRIBUTES','EMP_ID',current_timestamp(),'STRY0459509');
        call rls_policy_apply_sp('HRDP_CORE_{{ env_var('DBT_REGION') }}_DB','PMG_CORE_SCH','YE_FORM_ATTRIBUTES');
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}