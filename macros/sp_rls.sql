{% macro sp_rls() %}

    {% set query %}
        -------------- Prerequisite to Set the Ground ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        USE DATABASE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        USE SCHEMA CMN_CORE_SCH;
        -------------- RLS related Procedures --------------
        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.RLS_POLICY_APPLY_SP("DB_NAME" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "TBL_NAME" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Apply RLS on specific table'
        EXECUTE AS CALLER
        AS '
        var qBuildRLSAlter = `select distinct
        ''ALTER TABLE ''||DPRS_DATABASE_DPRS||''.''||DPRS_SCHEMA_DPRS||''.''||DPRS_TABLE_DPRS||''
        ADD ROW ACCESS POLICY CMN_CORE_SCH.''||REPLACE(DPRS_SUBJECT_DOMAIN_DPRS,''-'',''_'')||''_RLS_POL ON (''||DPRS_REF_COLUMN_DPRS||'');'' AlterQuery
        FROM CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS
        WHERE DPRS_DATABASE_DPRS=''`+DB_NAME+`'' and DPRS_SCHEMA_DPRS=''`+SCHEMA_NAME+`'' and DPRS_TABLE_DPRS=UPPER(''`+TBL_NAME+`'')`;
        var rBuildRLSAlter =  snowflake.execute({sqlText: qBuildRLSAlter });
        if (rBuildRLSAlter.getRowCount() > 0) {
            while (rBuildRLSAlter.next()) {
                try {
                    var qRLSAlter= rBuildRLSAlter.getColumnValue(1);
                    snowflake.execute({sqlText: qRLSAlter });
                }
                catch (err) {  
                    result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
                    result += "\\n  Message: " + err.message;
                    result += "\\nStack Trace:\\n" + err.stackTraceTxt; 
                    if ( result.includes(''Only one ROW_ACCESS_POLICY is allowed at a time'') ) { 
                        return ''RLS Policy already exist for ''+DB_NAME+''.''+SCHEMA_NAME+''.''+TBL_NAME;
                    }
                    else {
                        return result
                    }
                }
            }
            return ''RLS Policy created for ''+DB_NAME+''.''+SCHEMA_NAME+''.''+TBL_NAME;
        } 
        else {
            return ''RLS Policy not configured for ''+DB_NAME+''.''+SCHEMA_NAME+''.''+TBL_NAME+''in CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS Table'';
        }
        ';

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.RLS_POLICY_APPLY_ALL_SP("DB_NAME" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Create All RLS Policies for given DB and Schema'
        EXECUTE AS CALLER
        AS '
        var ApplyRLSAll = `select DISTINCT DPRS_DATABASE_DPRS,DPRS_SCHEMA_DPRS,DPRS_TABLE_DPRS from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where DPRS_DATABASE_DPRS like ''`+ DB_NAME +`'' and DPRS_SCHEMA_DPRS like ''`+ SCHEMA_NAME +`'';`;
        var ApplyRLS = snowflake.execute({sqlText: ApplyRLSAll });
        var returnVal = ``;
        while (ApplyRLS.next())
            {   var v_db_nm= ApplyRLS.getColumnValue(1);
                var v_sch_nm= ApplyRLS.getColumnValue(2);
                var v_tbl_nm= ApplyRLS.getColumnValue(3);
                var CallApplyRLS=`call cmn_core_sch.rls_policy_apply_sp(''`+ v_db_nm +`'',''`+ v_sch_nm +`'',''`+ v_tbl_nm +`'');`
                rCallApplyRLS=snowflake.execute({sqlText: CallApplyRLS });
                rCallApplyRLS.next();
                var returnVal = returnVal + rCallApplyRLS.getColumnValue(1) + `\\n`;
            }
        return returnVal
        ';
    {% endset %}
    
    {% do run_query(query) %}

{% endmacro %}