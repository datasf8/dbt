{% macro sp_masking() %}

    {% set query %}
        -------------- Prerequisite to Set the Ground ---------
        USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        USE DATABASE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        USE SCHEMA CMN_CORE_SCH;

        -------------- Masking related Procedures --------------

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.MASKING_TAG_CREATE_SP()
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Create tags associated to Masking Policies'
        EXECUTE AS CALLER
        AS '
        var list_of_tags = `SELECT DISTINCT DPTC_TAG_NAME_DPTC FROM DIM_PARAM_TAGS_COLUMNS`;
        var list_of_tags_res = snowflake.execute({
            sqlText: list_of_tags
        });
        while (list_of_tags_res.next()) {
            var tag_name = list_of_tags_res.getColumnValue(1);
            var my_sql_command = `select ''CREATE TAG ` + tag_name + ` ALLOWED_VALUES''|| VAL from (select LISTAGG( distinct \\''\\\\''\\''||DPTC_TAG_VALUE_DPTC||\\''\\\\''\\'', '','') as VAL from CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS WHERE DPTC_TAG_NAME_DPTC=''` + tag_name + `'')`;
            var my_sql_command_res = snowflake.execute({
                sqlText: my_sql_command
            });
            while (my_sql_command_res.next()) {
                var create_stm = my_sql_command_res.getColumnValue(1);
                try {
                    snowflake.execute({
                        sqlText: create_stm
                    });
                } catch (err) {
                    var my_sql_command2 = `select ''ALTER TAG ` + tag_name + ` ADD ALLOWED_VALUES''|| VAL from (select  distinct  \\''\\\\''\\''||DPTC_TAG_VALUE_DPTC||\\''\\\\''\\'' as VAL from CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS WHERE DPTC_TAG_NAME_DPTC=''` + tag_name + `'')`;
                    var my_sql_command2_res = snowflake.execute({
                        sqlText: my_sql_command2
                    });
                    while (my_sql_command2_res.next()) {
                        var create_stm2 = my_sql_command2_res.getColumnValue(1);
                        try {
                            snowflake.execute({
                                sqlText: create_stm2
                            });
                        } catch (err) {}
                    }
                }
            }
        }
        return ''SUCCESS''
        ';

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.MASKING_POLICY_SET_PER_POLICY_SP("MASKING" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Set Masking Policies from all Tables by Tag Name and Value'
        EXECUTE AS CALLER
        AS '
        var alter_masking_list = `select distinct ''ALTER TABLE ''||DPTC_DATABASE_DPTC||''.''||DPTC_SCHEMA_DPTC||''.''||DPTC_TABLE_DPTC||'' MODIFY COLUMN ''||DPTC_MASKING_COLUMN_DPTC||'' SET MASKING POLICY ''||MASKING_NAME||'' USING (''||DPTC_MASKING_COLUMN_DPTC||'',''||DPTC_REF_COLUMN_DPTC||'')'' FROM (SELECT *,''` + MASKING + `'' as MASKING_NAME FROM CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS WHERE DPTC_TAG_NAME_DPTC||''_''||DPTC_TAG_VALUE_DPTC||''_''||DPTC_DATA_TYPE_COLS||''_MASKING''=''` + MASKING + `'')`;
        var alter_masking = snowflake.execute({
            sqlText: alter_masking_list
        });
        while (alter_masking.next()) {
            try {
            snowflake.createStatement({
                sqlText: alter_masking.getColumnValue(1)
            }).execute();
            }
            catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result


            }
        }
        return ''SUCCESS''
        ';

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.MASKING_POLICY_UNSET_PER_POLICY_SP("MASKING" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Unset Masking Policies from all Tables by Tag Name and Value'
        EXECUTE AS CALLER
        AS '
        var alter_masking_list = `select distinct ''ALTER TABLE ''||DPTC_DATABASE_DPTC||''.''||DPTC_SCHEMA_DPTC||''.''||DPTC_TABLE_DPTC||'' MODIFY COLUMN ''||DPTC_MASKING_COLUMN_DPTC||'' UNSET MASKING POLICY  '' FROM (SELECT *, ''` + MASKING + `'' as MASKING_NAME FROM CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS WHERE DPTC_TAG_NAME_DPTC||''_''||DPTC_TAG_VALUE_DPTC||''_''||DPTC_DATA_TYPE_COLS||''_MASKING''=''` + MASKING + `'')`;
        var alter_masking = snowflake.execute({
            sqlText: alter_masking_list
        });
        while (alter_masking.next()) {
            try {
            snowflake.createStatement({
                sqlText: alter_masking.getColumnValue(1)
            }).execute();
            }
            catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result


            }
        }
        return ''SUCCESS''
        ';

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.MASKING_TAG_SET_SP("DB_NM" VARCHAR(16777216), "SHC_NM" VARCHAR(16777216), "TABLE_NM" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Set Masking Tags by Table'
        EXECUTE AS CALLER
        AS '
        var call1 = `call cmn_core_sch.masking_tag_create_sp();`
        try {
            snowflake.execute({
                sqlText: call1
            });
        } catch (err) {
            result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
            result += "\\n  Message: " + err.message;
            result += "\\nStack Trace:\\n" + err.stackTraceTxt;
            return result
        }
        var alter_tag_list = `select distinct ''ALTER TABLE ''||DPTC_DATABASE_DPTC||''.''||DPTC_SCHEMA_DPTC||''.''||DPTC_TABLE_DPTC||'' MODIFY COLUMN ''||DPTC_MASKING_COLUMN_DPTC||'' SET TAG ''||DPTC_TAG_NAME_DPTC||'' =\\\\''''||DPTC_TAG_VALUE_DPTC||''\\\\'';'' FROM (SELECT *, DPTC_TAG_NAME_DPTC as TAG_NAME FROM CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS WHERE DPTC_DATABASE_DPTC=''` + DB_NM + `'' AND DPTC_SCHEMA_DPTC=''` + SHC_NM + `'' AND DPTC_TABLE_DPTC=''` + TABLE_NM + `'')`;
        var alter_tag = snowflake.execute({
            sqlText: alter_tag_list
        });
        while (alter_tag.next()) {
            try {
                snowflake.createStatement({
                    sqlText: alter_tag.getColumnValue(1)
                }).execute();
            } catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result
            }
        }
        return ''SUCCESS''
        ';
 
        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.MASKING_POLICY_CREATE_SP("MASKING" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Create Masking Policies'
        EXECUTE AS CALLER
        AS '  
        var CREATE_MASKING_LISTS = `select DISTINCT 
        ''create or replace masking policy ''||MASKING_NAME||'' AS (val ''||DPTC_DATA_TYPE_COLS||'',ref_col VARCHAR) returns ''||DPTC_DATA_TYPE_COLS||'' -> case when SYSTEM$GET_TAG_ON_CURRENT_COLUMN(\\\\''''||TAG_NAME||''\\\\'') = \\\\''''|| TAG_VALUE ||''\\\\'' then 
                case when ref_col is null 
                        then val
                    when exists (select 1 from CMN_CORE_SCH.DIM_PARAM_SNOWFLAKE_ROLES 
                                    where DPSR_SNOWFLAKE_ROLE_DPSR=current_role() and DPSR_EXCLUDE_SECURITY_DPSR=''''Y'''')
                        then val
                    when exists (select 1 from CMN_CORE_SCH.rel_sf_group_rls_user a
					                      join CMN_CORE_SCH.REL_GROUP_MASKING_USER_TAG b on a.GRP_ID = b.REUR_GROUP_ID_DDEP
                                    where         employee_upn = nvl(
                                                                        (
                                                                        select copy_from_user
                                                                        from cmn_core_sch.dim_param_security_replication
                                                                        where copy_to_user = current_user
                                                                        ),
                                                                        current_user
                                                                        ) 
                                        and REUR_TAG_VALUE_DPTC= \\\\''''|| TAG_VALUE ||''\\\\''
                                        and REUR_EMPLOYEE_ID_DDEP=''''ALL'''') 
                        then val
                    when exists (select 1 from CMN_CORE_SCH.rel_sf_group_rls_user a
					                      join CMN_CORE_SCH.REL_GROUP_MASKING_USER_TAG b on a.GRP_ID = b.REUR_GROUP_ID_DDEP
                                    where         employee_upn = nvl(
                                                                        (
                                                                        select copy_from_user
                                                                        from cmn_core_sch.dim_param_security_replication
                                                                        where copy_to_user = current_user
                                                                        ),
                                                                        current_user
                                                                        ) 
                                        and REUR_TAG_VALUE_DPTC= \\\\''''|| TAG_VALUE ||''\\\\''
                                        and REUR_EMPLOYEE_ID_DDEP!=''''ALL'''' and REUR_EMPLOYEE_ID_DDEP=ref_col) 
                        then val
                    else ''|| DPTC_MASKING_VALUE_DPTC ||''
                end
            else NULL
        end;'',MASKING_NAME
        from (select distinct DPTC_DATA_TYPE_COLS,DPTC_TAG_VALUE_DPTC AS TAG_VALUE,DPTC_TAG_NAME_DPTC AS TAG_NAME
                ,DPTC_TAG_NAME_DPTC||''_''||DPTC_TAG_VALUE_DPTC||''_''||DPTC_DATA_TYPE_COLS||''_MASKING'' as MASKING_NAME
                ,iff(DPTC_MASKING_VALUE_DPTC is null,''NULL'',''cast(''''''||DPTC_MASKING_VALUE_DPTC||'''''' as ''||DPTC_DATA_TYPE_COLS||'')'') AS DPTC_MASKING_VALUE_DPTC
                from CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS where MASKING_NAME=''` + MASKING + `'');`;
        var CREATE_MASKING_LISTS_EX = snowflake.execute({
            sqlText: CREATE_MASKING_LISTS
        });
        while (CREATE_MASKING_LISTS_EX.next()) {
            var v_create = CREATE_MASKING_LISTS_EX.getColumnValue(1);
            var v_masking = CREATE_MASKING_LISTS_EX.getColumnValue(2);
            try {
                var call1 = `call cmn_core_sch.masking_tag_create_sp();`
                snowflake.execute({
                    sqlText: call1
                });

            } catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result
            }
            try {
                var call2 = `call cmn_core_sch.masking_policy_unset_per_policy_sp(''` + v_masking + `'');`
                snowflake.execute({
                    sqlText: call2
                });
            } catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result
            }
            try {
                snowflake.execute({
                    sqlText: v_create
                });
            } catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result
            }
            try {
            var call3 = `call cmn_core_sch.masking_policy_set_per_policy_sp(''` + v_masking + `'');`
            snowflake.execute({
                sqlText: call3
            });
            }
            catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result
            }
        }
        return ''SUCCESS''
        ';

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.MASKING_POLICY_CREATE_ALL_SP("DB_NM" VARCHAR(16777216), "SCHEMA_NM" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Create All Masking Policies for given DB and Schema'
        EXECUTE AS CALLER
        AS '
        var CreatMaskingAll = `select DISTINCT DPTC_TAG_NAME_DPTC||''_''||DPTC_TAG_VALUE_DPTC||''_''||DPTC_DATA_TYPE_COLS||''_MASKING'' as MASKING_NAME from CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS where DPTC_DATABASE_DPTC like ''` + DB_NM + `'' and DPTC_SCHEMA_DPTC like ''` + SCHEMA_NM + `'';`;
        var CreatMasking = snowflake.execute({
            sqlText: CreatMaskingAll
        });
        while (CreatMasking.next()) {
            try {
            var v_masking = CreatMasking.getColumnValue(1);
            var CallCreateMasking = `call cmn_core_sch.masking_policy_create_sp(''` + v_masking + `'');`
            snowflake.execute({
                sqlText: CallCreateMasking
            });
            }
            catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result
            }
        }
        return ''SUCCESS''
        ';

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.MASKING_POLICY_APPLY_SP("DB_NM" VARCHAR(16777216), "SHC_NM" VARCHAR(16777216), "TABLE_NM" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Apply Masking Policies per Table'
        EXECUTE AS CALLER
        AS '
        var callTagCreate = `call cmn_core_sch.masking_tag_set_sp(''` + DB_NM + `'',''` + SHC_NM + `'',''` + TABLE_NM + `'');`
        snowflake.execute({
            sqlText: callTagCreate
        });
        var alter_masking_list = `select distinct 
        ''ALTER TABLE ` + DB_NM + `.` + SHC_NM + `.` + TABLE_NM + ` MODIFY COLUMN ''||DPTC_MASKING_COLUMN_DPTC||'' 
        SET MASKING POLICY ''||MASKING_NAME||'' USING (''||DPTC_MASKING_COLUMN_DPTC||'',''||DPTC_REF_COLUMN_DPTC||'')'',MASKING_NAME 
        FROM (SELECT *, DPTC_TAG_NAME_DPTC||''_''||DPTC_TAG_VALUE_DPTC||''_''||DPTC_DATA_TYPE_COLS||''_MASKING'' as MASKING_NAME FROM CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS WHERE DPTC_DATABASE_DPTC=''` + DB_NM + `'' AND DPTC_SCHEMA_DPTC=''` + SHC_NM + `'' AND DPTC_TABLE_DPTC=''` + TABLE_NM + `'')`;
        var alter_masking = snowflake.execute({
            sqlText: alter_masking_list
        });
        while (alter_masking.next()) {
            try {
                var v_masking = alter_masking.getColumnValue(2);
                var v_query = alter_masking.getColumnValue(1);
                snowflake.execute({
                    sqlText: v_query
                });
            } catch (err) {
                // if masking policy does not exist then call masking masking_policy_create_sp. it will create the making policy
                // then we run the first statement again to alter the table 
                // finally we apply tags
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                if (result.includes(''Code: 2003'')) {
                    try {
                        var call1 = `call cmn_core_sch.masking_policy_create_sp(''` + v_masking + `'');`
                        snowflake.execute({
                            sqlText: call1
                        });
                    } catch (err) {
                        result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                        result += "\\n  Message: " + err.message;
                        result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                        return result
                    }

                } else {
                    // if other errors then error out
                    return result
                }
            }
        }
        return ''SUCCESS''
        ';

        CREATE OR REPLACE PROCEDURE CMN_CORE_SCH.MASKING_POLICY_APPLY_ALL_SP("DB_NM" VARCHAR(16777216), "SCHEMA_NM" VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE JAVASCRIPT
        COMMENT='Create All Masking Policies for given DB and Schema'
        EXECUTE AS CALLER
        AS '
        var ApplyMaskingAll = `select DISTINCT DPTC_DATABASE_DPTC,DPTC_SCHEMA_DPTC,DPTC_TABLE_DPTC from CMN_CORE_SCH.DIM_PARAM_TAGS_COLUMNS where DPTC_DATABASE_DPTC like ''` + DB_NM + `'' and DPTC_SCHEMA_DPTC like ''` + SCHEMA_NM + `'';`;
        var ApplyMasking = snowflake.execute({
            sqlText: ApplyMaskingAll
        });
        while (ApplyMasking.next()) {
            var v_db_nm = ApplyMasking.getColumnValue(1);
            var v_sch_nm = ApplyMasking.getColumnValue(2);
            var v_tbl_nm = ApplyMasking.getColumnValue(3);
            try {
                var CallApplyMasking = `call cmn_core_sch.masking_policy_apply_sp(''` + v_db_nm + `'',''` + v_sch_nm + `'',''` + v_tbl_nm + `'');`
                snowflake.execute({
                    sqlText: CallApplyMasking
                });

            } catch (err) {
                result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
                result += "\\n  Message: " + err.message;
                result += "\\nStack Trace:\\n" + err.stackTraceTxt;
                return result
            }

        }
        return ''SUCCESS''
        ';

        call cmn_core_sch.masking_policy_create_all_sp('%','%');
        call cmn_core_sch.masking_policy_apply_all_sp('%','%');
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
