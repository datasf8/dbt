{% macro STRY0480887_pos_hire_rls() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        -- Delete and Insert of Roles into Param SF Roles
        delete from HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where DPRS_TABLE_DPRS = 'FACT_POSITION';
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('POS_HIRE','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_POSITION','BUSINESS_UNIT_CODE,SPECIALIZATION_CODE',current_timestamp(),'STRY0480887');
        Alter Table HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_POSITION drop all row access policies ;
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');
        call cmn_core_sch.rls_policy_apply_all_sp('HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH');
        
    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}