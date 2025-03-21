{% macro sp_impersonate() %}

    {% set query %}
        -------------- Prerequisite to Set the Ground ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        USE DATABASE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        USE SCHEMA CMN_CORE_SCH;
        -------------- Impersonate Users --------------
        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.SECURITY_REPLICATION_SP()
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Replace user from RelEmployeeUser and RelEmployeeUserTag for testing RLS and Masking Policy'
        EXECUTE AS CALLER
        AS '
            var vDBName = `hrdp_core_{{ env_var('DBT_REGION') }}_db`;
            var qGetUsers = `select copy_from_user,copy_to_user from `+vDBName+`.cmn_core_sch.dim_param_security_replication where (replication_ts is null or current_user like ''SFPYTHON%'' or current_user() like ''%_DBT_%'')`;
            var rGetUsers = snowflake.execute({sqlText: qGetUsers});
            while (rGetUsers.next()) {
                var FROM_USER = rGetUsers.getColumnValue(1);
                var TO_USER = rGetUsers.getColumnValue(2);
                var qDeleteUser = `delete from `+vDBName+`.cmn_core_sch.rel_employee_user where reeu_access_employee_upn_ddep=''`+ TO_USER +`''`;
                snowflake.execute({sqlText: qDeleteUser});
                var qInsertUser = `insert into `+vDBName+`.cmn_core_sch.rel_employee_user
                    select REEU_SUBJECT_DOMAIN_REEU,''Impersonate from ''||REEU_ACCESS_EMPLOYEE_ID_REEM,''`+ TO_USER +`'',REEU_EMPLOYEE_ID_DDEP
                    from `+vDBName+`.cmn_core_sch.rel_employee_user where reeu_access_employee_upn_ddep=''`+ FROM_USER +`''`;
                snowflake.execute({sqlText: qInsertUser});
                var qDeleteUserTag = `delete from `+vDBName+`.cmn_core_sch.rel_employee_user_tag where reur_access_employee_upn_ddep=''`+ TO_USER +`''`;
                snowflake.execute({sqlText: qDeleteUserTag});
                var qInsertUserTag = `insert into `+vDBName+`.cmn_core_sch.rel_employee_user_tag
                    select ''`+ TO_USER +`'',REUR_EMPLOYEE_ID_DDEP,REUR_TAG_VALUE_DPTC
                    from `+vDBName+`.cmn_core_sch.rel_employee_user_tag where reur_access_employee_upn_ddep=''`+ FROM_USER +`''`;
                snowflake.execute({sqlText: qInsertUserTag});
                var qDeleteUserPM = `delete from `+vDBName+`.cmn_core_sch.dim_position_access where ACCESS_EMPLOYEE_UPN=''`+ TO_USER +`''`;
                snowflake.execute({sqlText: qDeleteUserPM});
                var qInsertUserPM = `insert into `+vDBName+`.cmn_core_sch.dim_position_access
                    select ''Impersonate from ''||USER_ID,''`+ TO_USER +`'',SCOPE_TYPE,COUNTRY,ZONE,SCOPE_ACCESS
                    from `+vDBName+`.cmn_core_sch.dim_position_access where ACCESS_EMPLOYEE_UPN=''`+ FROM_USER +`''`;
                snowflake.execute({sqlText: qInsertUserPM});
                var qDeleteUserCLS = `delete from `+vDBName+`.cmn_core_sch.rel_cls_employee_dashboard where employee_upn=''`+ TO_USER +`''`;
                snowflake.execute({sqlText: qDeleteUserCLS});
                var qInsertUserCLS = `insert into `+vDBName+`.cmn_core_sch.rel_cls_employee_dashboard
                    select report_name,''Impersonate from ''||EMPLOYEE_ID,''`+ TO_USER +`'',field_name,visibility
                    from `+vDBName+`.cmn_core_sch.rel_cls_employee_dashboard where employee_upn=''`+ FROM_USER +`''`;
                snowflake.execute({sqlText: qInsertUserCLS});
                var qDeleteUserCBU = `delete from `+vDBName+`.cmn_core_sch.rel_company_by_user where user_upn=''`+ TO_USER +`''`;
                snowflake.execute({sqlText: qDeleteUserCBU});
                var qInsertUserCBU = `insert into `+vDBName+`.cmn_core_sch.rel_company_by_user
                    select subject_domain,''Impersonate from ''||user_id,''`+ TO_USER +`'',company_code
                    from `+vDBName+`.cmn_core_sch.rel_company_by_user where user_upn=''`+ FROM_USER +`''`;
                snowflake.execute({sqlText: qInsertUserCBU});
                var qDeleteUserBSU = `delete from `+vDBName+`.cmn_core_sch.rel_bu_spec_by_user where user_upn=''`+ TO_USER +`''`;
                snowflake.execute({sqlText: qDeleteUserBSU});
                var qInsertUserBSU = `insert into `+vDBName+`.cmn_core_sch.rel_bu_spec_by_user
                    select ''Impersonate from ''||user_id,''`+ TO_USER +`'',business_unit_code,specialization_code
                    from `+vDBName+`.cmn_core_sch.rel_bu_spec_by_user where user_upn=''`+ FROM_USER +`''`;
                snowflake.execute({sqlText: qInsertUserBSU});
            }
            var qUpdTable = `update `+vDBName+`.cmn_core_sch.dim_param_security_replication set replication_ts=current_timestamp() where (replication_ts is null or current_user like ''SFPYTHON%'' or current_user() like ''%_DBT_%'')`;
            var rUpdTable = snowflake.execute({sqlText: qUpdTable});
        return ''SUCCESS'';
        ';

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.IMPERSONATE_SP("FROM_USER" VARCHAR(16777216), "TO_USER" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Insert into dim_param_security_replication for Security Replication or Impersonate the user'
        EXECUTE AS OWNER
        AS '
            var vDBName = `hrdp_core_{{ env_var('DBT_REGION') }}_db`;
            var qVerifyUsers = `select name from snowflake.account_usage.users where deleted_on is null and disabled = false and NAME ilike ''`+FROM_USER+`''`;
            var rVerifyUsers = snowflake.execute({sqlText: qVerifyUsers});
            var rcVerifyUsers = rVerifyUsers.getRowCount();
            if (rcVerifyUsers == 1) { while (rVerifyUsers.next()) { var vFromUser = rVerifyUsers.getColumnValue(1); } }
            else { return "Failed: " + FROM_USER + " is missing/disabled/deleted in Snowflake";}
            var qVerifyUsers = `select name from snowflake.account_usage.users where deleted_on is null and disabled = false and NAME ilike ''`+TO_USER+`''`;
            var rVerifyUsers = snowflake.execute({sqlText: qVerifyUsers});
            var rcVerifyUsers = rVerifyUsers.getRowCount();
            if (rcVerifyUsers == 1) { while (rVerifyUsers.next()) { var vToUser = rVerifyUsers.getColumnValue(1); } }
            else { return "Failed: " + TO_USER + " is missing/disabled/deleted in Snowflake";}
            var qDeleteUsers = `delete from `+vDBName+`.cmn_core_sch.dim_param_security_replication where copy_to_user=''`+vToUser+`''`;
            snowflake.execute({sqlText: qDeleteUsers});
            if ( vFromUser == vToUser ) {
                return "Success: " + vToUser + " will back to regular access within next 2 hours (after next load)";
            }
            else {
                var qInsertUsers = `insert into `+vDBName+`.cmn_core_sch.dim_param_security_replication(copy_from_user,copy_to_user) values (''`+vFromUser+`'',''`+vToUser+`'')`;
                snowflake.execute({sqlText: qInsertUsers});
                var qCallReplication = `call `+vDBName+`.cmn_core_sch.security_replication_sp();`;
                snowflake.execute({sqlText: qCallReplication});
                return "Success: Impersonate of " + vFromUser + " to " + vToUser + " is completed.";
            }
        ';

        grant ownership
            on procedure cmn_core_sch.impersonate_sp(varchar,varchar) 
            to role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN COPY CURRENT GRANTS;
        grant all 
            on procedure cmn_core_sch.impersonate_sp(VARCHAR, VARCHAR) 
            to role _TECH_CORE_CMN_{{ env_var('DBT_REGION') }}_OWN_ROLE;
        grant usage 
            on procedure cmn_core_sch.impersonate_sp(VARCHAR, VARCHAR) 
            to role HRDP_SECURITY_REPLICATION;

    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
