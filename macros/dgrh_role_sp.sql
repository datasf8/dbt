{% macro dgrh_role_sp() %}

{% set query %}
    -------------- Prerequisite to Set the Ground ---------
    USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
    USE DATABASE HRDP_ADM_{{ env_var('DBT_REGION') }}_DB;
    USE SCHEMA DB_SCHEMA_SCH;
    -------------- Impersonate Users --------------
    CREATE OR REPLACE PROCEDURE HRDP_ADM_{{ env_var('DBT_REGION') }}_DB.DB_SCHEMA_SCH.DGRH_ROLE_SP()
    RETURNS VARCHAR(16777216)
    LANGUAGE JAVASCRIPT
    COMMENT='Stored Procedure to grant Database Access Role to DGRH Role'
    EXECUTE AS CALLER
    AS '
    let v_env="{{ env_var('DBT_REGION') }}";

    //Give access to PUB schemas
    var list_grants=`
        with drm as (
            select distinct name as role_name,database_name,
                database_name||''.DAR_''||split_part(database_name,''_'',2)
                    ||iff(split_part(database_name,''_'',2)=''SDDS'','''',''_''||split_part(schema_name,''_'',1))||''_RO'' dar_name
            from  HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.dgrh_role_mapping rm
            join (select decode(''`+v_env+`'',''PD'',''PRD'',''QA'',''QUAL'',''DV'',''DEV'',''`+v_env+`'') environment) env
            join HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.schema_table st
                on (split_part(database_name,''_'',2)=''PUB'' and SHORT_NAME!=''SDDS''
                    and (split_part(schema_name,''_'',1)=SHORT_NAME or left(schema_name,3)=''CMN''))
                or (split_part(database_name,''_'',2)=''SDDS'' and SHORT_NAME=''SDDS'')
            join snowflake.account_usage.roles on name = (''DGRH-PAPLATFORM-''||LONG_NAME||''-''||environment) and deleted_on is null
            minus select role_name,database_name,dar_name from HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.role_dar_mapping
        )
        select ''grant database role ''||dar_name||'' to role "''||role_name||''";'' grnt_qry
            ,''insert into HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.role_dar_mapping(role_name,database_name,dar_name,sp_name)
                values(''''''||role_name||'''''',''''''||database_name||'''''',''''''||dar_name||'''''',''''DGRH_ROLE_SP'''');''
        from drm;`;
    var list_grants_result=snowflake.createStatement( {sqlText: list_grants} ).execute()
    
    //Looping through the databases
    while (list_grants_result.next()) { 
        var grnt=list_grants_result.getColumnValue(1);
        snowflake.createStatement( {sqlText: grnt} ).execute();
        var insrt=list_grants_result.getColumnValue(2);
        snowflake.createStatement( {sqlText: insrt} ).execute()
    }

    return "Success"

    ';

{% endset %}

{% do run_query(query) %}

{% endmacro %}
