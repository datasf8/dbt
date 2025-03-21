{% macro stry0430131_hire_rls() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        -- Create clone of EC RLS Table for HIRE RLS Table, Just to avoid execution
        create table if not exists cmn_core_sch.rel_hire_sf_group_rls_employee clone cmn_core_sch.rel_ec_sf_group_rls_employee;

        -- Delete and Insert of Roles into Param SF Roles
        delete from HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES where subjectdomain = 'HIRE';
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,800,'HR10 - HR PROFESSIONAL (HR ROLE 2)',999);
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,1337,'HR25 - TALENT ACQUISITION',999);

        -- Delete and Insert of tables with Policy into RLS Setup Table
        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0430131';
        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where DPRS_TABLE_DPRS='FACT_RECRUITMENT';
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('HIRE','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_HIRES','USER_ID',current_timestamp(),'STRY0430131');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('HIRE','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_RECRUITMENT','USER_ID',current_timestamp(),'STRY0430131');
        alter table HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_RECRUITMENT drop all row access policies;
        
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');
        --call cmn_core_sch.rls_policy_apply_all_sp('HRDP_CORE_{{ env_var('DBT_REGION') }}_DB','CMN_CORE_SCH');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
