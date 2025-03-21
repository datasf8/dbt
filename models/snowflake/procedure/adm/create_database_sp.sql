{{
    config(
        materialized="view",
        transient=false,
        post_hook ="DROP VIEW {{ this }};",
        pre_hook ="
CREATE OR REPLACE PROCEDURE HRDP_ADM_{{ env_var('DBT_REGION') }}_DB.DB_SCHEMA_SCH.CREATE_DATABASE_SP(DATABASE_NAME VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
COMMENT='Stored Procedure to manage Database with Database Access Role (DAR) creation at DB level'
EXECUTE AS CALLER
AS '
    //Collection of Inputs & Configurations
    let v_env=''{{ env_var('DBT_REGION') }}'';
    let v_db = DATABASE_NAME.toUpperCase();
    let v_db_dar_part=v_db+''.DAR_''+v_db.split(''_'')[1]+''_''; //Part of DAR (Database Access Roles)
    switch (v_env) {
        case ''PD'':    var v_retention = 30; break;
        default:      var v_retention = 10; break;    
    }
    let v_own_rl = ''HRDP_''+v_env+''_DOMAIN_ADMIN'';
    //Exception for DB Creation with different Ownership
    if (v_db.split(''_'')[1]==''DATAQUALITY'') { v_own_rl=''HRDP_DATA_QUALITY_ROLE''; }
    
    //Check Database and Schema Name Standard
    if (v_db!=''HRDP_EXTERNAL_DB'' //Exception for Database at Account Level
            && (v_db.split(''_'')[0]!=''HRDP'' || v_db.split(''_'')[2]!=v_env || v_db.split(''_'')[3]!=''DB'')) {
        return ''Database name should have HRDP_XXXX_DV/QA/NP/PD_DB, Check the Current Database in Use'';
    }

    //Create Database and DAR (ALL, RW & RO)
    snowflake.createStatement( {sqlText: `create database IF NOT EXISTS `+v_db+` DATA_RETENTION_TIME_IN_DAYS=`+v_retention+`;`} ).execute();
    snowflake.createStatement( {sqlText: `grant ALL on database `+v_db+` to role `+v_own_rl+`;`} ).execute();
    snowflake.createStatement( {sqlText: `grant OWNERSHIP on all schemas in database `+v_db+` to role `+v_own_rl+` copy current grants;`} ).execute();
    
    //Insert an entry to Table for validation
    var insert_into_database_table=`
        merge into HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.database_table 
            using (select ''`+v_db+`'' db_name,''`+v_own_rl+`'' own_rl) on database_name=db_name
        when MATCHED then UPDATE SET owner_role = own_rl,updated_by=current_user,updated_at = current_timestamp
        when NOT MATCHED then INSERT (database_name,owner_role) VALUES (db_name,own_rl)`;
    snowflake.createStatement( {sqlText: insert_into_database_table} ).execute();
    
    return ''Success: Database ''+v_db+'' is created/reconfigured'';

    ';"       
    )
}}
select 1 as a
from dual
