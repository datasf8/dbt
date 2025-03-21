{% macro create_schema_sp() %}

    {% set query %}
    -------------- Prerequisite to Set the Ground ---------
    USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
    USE DATABASE HRDP_ADM_{{ env_var('DBT_REGION') }}_DB;
    USE SCHEMA DB_SCHEMA_SCH;
    -------------- Impersonate Users --------------
    CREATE OR REPLACE PROCEDURE HRDP_ADM_{{ env_var('DBT_REGION') }}_DB.DB_SCHEMA_SCH.CREATE_SCHMEA_SP("DATABASE_NAME" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216))
    RETURNS VARCHAR(16777216)
    LANGUAGE JAVASCRIPT
    COMMENT='Stored Procedure to manage Database, Scehmas, Database Access roles to be created'
    EXECUTE AS CALLER
    AS '
        //Collection of Inputs & Configurations
        let v_env="{{ env_var('DBT_REGION') }}";
        let v_db = DATABASE_NAME.toUpperCase();
        let v_sch = SCHEMA_NAME.toUpperCase();
        let v_db_dar_part=v_db+''.DAR_''+v_db.split("_")[1]+''_''; //Part of DAR (Database Access Roles)
        let v_sch_dar_part=v_db_dar_part+v_sch.split("_")[0]+''_''; //Will be modified if i.e. for SDDS Database as C1/C2 etc.

        //Identify Schema Owner from database_table
        var own_qry=`select owner_role from HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.database_table where database_name=''`+v_db+`'';`;
        var own_qry_result = snowflake.createStatement( {sqlText: own_qry} ).execute();
        while (own_qry_result.next()) { var v_own_rl = own_qry_result.getColumnValue(1); }

        //Check Database and Schema Name Standard
        if (v_db.split("_")[0]!="HRDP" || v_db.split("_")[2]!=v_env || v_db.split("_")[3]!="DB") {
            return "Database name should have HRDP_XXXX_DV/QA/NP/PD_DB, Check the Current Database in Use";
        } else if (v_db.split("_")[1]=="SDDS") {
            if(v_sch.match(/^BTDP_DS_C._H..*$/g) //Matching Starting piece of SDDS Schema name
                && (v_sch.substr(v_sch.length-2,2)==v_env || v_sch.substr(v_sch.length-10,10)==v_env+''_PRIVATE'')) {
                v_sch_dar_part=v_db+''.DAR_SDDS_''+v_sch.split("_")[2]+''_'';
            }
            else {
                return "SDDS Schema name should Start with BTDP_DS_C1/C2/C3 and end with "+v_env+" or "+v_env+"_PRIVATE";
            }
        } else if (v_sch.substr(v_sch.length-3,3)!="SCH") {
            return "Schema name should end with _SCH"; 
        }
        
        //Create DAR at Database Level and Grant to HRDP_[DV|QA|NP|PD]_[ALL|RW|RO]_ROLE
        snowflake.createStatement( {sqlText: `create database role  IF NOT EXISTS `+v_db_dar_part+''ALL''+`;`} ).execute();
        snowflake.createStatement( {sqlText: `create database role  IF NOT EXISTS `+v_db_dar_part+''RW''+`;`} ).execute();
        snowflake.createStatement( {sqlText: `create database role  IF NOT EXISTS `+v_db_dar_part+''RO''+`;`} ).execute();
        snowflake.createStatement( {sqlText: `grant database role `+v_db_dar_part+`ALL to role HRDP_`+v_env+`_ALL_ROLE;`} ).execute();
        snowflake.createStatement( {sqlText: `grant database role `+v_db_dar_part+`RW to role HRDP_`+v_env+`_RW_ROLE;`} ).execute();
        snowflake.createStatement( {sqlText: `grant database role `+v_db_dar_part+`RO to role HRDP_`+v_env+`_RO_ROLE;`} ).execute();

        //Record DAR Grants in Table
        var ins_dar_qry=`
                insert into HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.role_dar_mapping(role_name,database_name,dar_name,sp_name)
                select ''HRDP_`+v_env+`_ALL_ROLE'',''`+v_db+`'',''`+v_db_dar_part+`ALL'',''CREATE_SCHEMA_SP''
                union all select ''HRDP_`+v_env+`_RW_ROLE'',''`+v_db+`'',''`+v_db_dar_part+`RW'',''CREATE_SCHEMA_SP''
                union all select ''HRDP_`+v_env+`_RO_ROLE'',''`+v_db+`'',''`+v_db_dar_part+`RO'',''CREATE_SCHEMA_SP''
                minus select * exclude(updated_by,updated_at) from HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.role_dar_mapping;`;
        snowflake.createStatement( {sqlText: ins_dar_qry} ).execute();

        //Create Schema and DAR (ALL, RW & RO)
        snowflake.createStatement( {sqlText: `create schema IF NOT EXISTS `+v_db+`.`+v_sch+` WITH MANAGED ACCESS;`} ).execute();
        if (v_db.split("_")[1]!="DATAQUALITY") {
            snowflake.createStatement( {sqlText: `create database role  IF NOT EXISTS `+v_sch_dar_part+''ALL''+`;`} ).execute();
            snowflake.createStatement( {sqlText: `create database role  IF NOT EXISTS `+v_sch_dar_part+''RW''+`;`} ).execute();
            snowflake.createStatement( {sqlText: `create database role  IF NOT EXISTS `+v_sch_dar_part+''RO''+`;`} ).execute();
            //Grant Schema Level DAR to DB Level DAR
            snowflake.createStatement( {sqlText: `grant database role `+v_sch_dar_part+`ALL to database role `+v_db_dar_part+`ALL;`} ).execute();
            snowflake.createStatement( {sqlText: `grant database role `+v_sch_dar_part+`RW to database role `+v_db_dar_part+`RW;`} ).execute();
            snowflake.createStatement( {sqlText: `grant database role `+v_sch_dar_part+`RO to database role `+v_db_dar_part+`RO;`} ).execute();
        } else {
            v_sch_dar_part=v_db_dar_part;
        }

        //Grants for Schema and their objects
        var grant_list=`
                select ''grant ALL on schema ''||''`+v_db+`.`+v_sch+` to role `+v_own_rl+`;'' qry
                union all select ''grant ALL on schema ''||''`+v_db+`.`+v_sch+` to database role `+v_sch_dar_part+`ALL;'' qry
                union all select ''grant USAGE on schema ''||''`+v_db+`.`+v_sch+` to database role `+v_sch_dar_part+`RW;'' qry
                union all select ''grant USAGE on schema ''||''`+v_db+`.`+v_sch+` to database role `+v_sch_dar_part+`RO;'' qry
                union all
                (select ''grant ''
                        ||case when o.$1 in(''VIEWS'',''TABLES'') and p.$1=''RW'' then ''SELECT,INSERT,UPDATE,DELETE'' 
                            when o.$1 in(''VIEWS'',''TABLES'') and p.$1=''RO'' then ''SELECT''
                            when o.$1=''DYNAMIC TABLES'' and p.$1 in (''RW'',''RO'')  then ''MONITOR''
                            when p.$1 in(''OWNERSHIP'',''ALL'') then p.$1
                            else ''USAGE'' end
                        ||'' on ''||ot.$1||'' ''||o.$1||'' in schema ''||''`+v_db+`.`+v_sch+`''
                        ||iff(p.$1=''OWNERSHIP'','' to role `+v_own_rl+`'','' to database role ''||''`+v_sch_dar_part+`''||p.$1)
                        ||iff(p.$1=''OWNERSHIP'' and ot.$1=''ALL'' and o.$1 not like ''%POLICIES'','' copy current grants;'','';'') qry
                from (values (''OWNERSHIP'',1),(''ALL'',2),(''RW'',3),(''RO'',4)) p
                join (values (''ALL'',1),(''FUTURE'',2)) ot
                join (values (''TABLES''),(''VIEWS''),(''DYNAMIC TABLES''),(''STAGES''),(''FILE FORMATS'')
                            ,(''PROCEDURES''),(''FUNCTIONS''),(''SEQUENCES'')) o
                where not (p.$1=''OWNERSHIP'' and ot.$1=''FUTURE'')
                order by p.$2,ot.$2); -- Grants for Owenership role and database access roles
            `;
        var grant_list_result = snowflake.createStatement( {sqlText: grant_list} ).execute();
        while (grant_list_result.next()) {
            var grant_statement = grant_list_result.getColumnValue(1);
            snowflake.createStatement( {sqlText: grant_statement} ).execute();
        }

        //Insert an entry to Table for validation
        var insert_into_schema_table=`
            merge into HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.schema_table 
                using (select ''`+v_db+`'' db_name,''`+v_sch+`'' sch_name,''`+v_own_rl+`'' own_rl) 
                on database_name=db_name and schema_name=sch_name
            when MATCHED then UPDATE SET owner_role = own_rl,updated_by=current_user,updated_at=current_timestamp
            when NOT MATCHED then INSERT (database_name,schema_name,owner_role) VALUES (db_name,sch_name,own_rl)`;
        snowflake.createStatement( {sqlText: insert_into_schema_table} ).execute();

        return "Success: Schema "+v_sch+" is created/reconfigured under Database "+v_db

        ';

    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
