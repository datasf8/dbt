{% macro param_hire_rls() %}
 
    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        -- Delete and Insert of Roles into Param SF Roles
        delete from HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES where subjectdomain = 'HIRE';
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,800,'HR10 - HR PROFESSIONAL (HR ROLE 2)',999);
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,1337,'HR25 - TALENT ACQUISITION',999);
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,917,'AD10 - IT ADMINISTRATOR - Pending Requests',999);
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,579,'AD10 - IT ADMINISTRATOR (EP)',999);
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,618,'AD20 - HRIS ZONE COORDINATOR (EP)',999);
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,1009,'AD30 - IT ZONE COORDINATOR (EC)',999);
        insert into HRDP_LND_{{ env_var('DBT_REGION') }}_DB.CMN_LND_SCH.PARAM_SF_ROLES values('HIRE',null,1,957,'DG10 - DATA GOUVERNANCE',999);

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}