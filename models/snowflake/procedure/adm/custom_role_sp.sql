{{
    config(
        materialized="view",
        transient=false,
        post_hook ="DROP VIEW {{ this }};",
        pre_hook ="
CREATE OR REPLACE PROCEDURE HRDP_ADM_{{ env_var('DBT_REGION') }}_DB.DB_SCHEMA_SCH.CUSTOM_ROLE_SP()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
COMMENT='Stored Procedure to grant Database Access Role to Custom Roles'
EXECUTE AS CALLER
AS '
    let v_env=''{{ env_var('DBT_REGION') }}'';

    //Give access to database roles
    var list_grants=`
        with crm as
        (select database_name||''.DAR_''||split_part(database_name,''_'',2)
                ||iff(schema_name=''ALL'' or split_part(database_name,''_'',2)=''DATAQUALITY'',''''
                    ,iff(split_part(database_name,''_'',2)=''SDDS'',''_''||schema_name,''_''||split_part(schema_name,''_'',1)))
                ||''_''||privileges_level as dar_name,cr.*
        from (
            select role_name,schema_name,privileges_level
                ,replace(database_name,''_PD_'',''_{{ env_var('DBT_REGION') }}_'') as database_name
            from HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.custom_role_mapping
            union all  
            select role_name,schema_name,privileges_level
                ,replace(database_name,''_PD_'',''_{{ env_var('DBT_REGION') }}_'') as database_name
            from HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.custom_role_mapping_seed) cr
        where exists(select 1 from HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.schema_table st
                    where cr.database_name=st.database_name
                        and (upper(cr.schema_name)=''ALL'' or cr.schema_name=st.schema_name
                            or (split_part(cr.database_name,''_'',2)=''SDDS'' and cr.schema_name=split_part(st.schema_name,''_'',3))))
        )
        select iff(rdm.dar_name is null,''grant database role ''||crm.dar_name||'' to role ''||crm.role_name||'';''
                ,''revoke database role ''||rdm.dar_name||'' from role ''||rdm.role_name||'';'') grnt_revk_qry
            ,iff(rdm.dar_name is null
                ,''insert into HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.role_dar_mapping(role_name,database_name,dar_name,sp_name)
                    values(''''''||crm.role_name||'''''',''''''||crm.database_name||'''''',''''''||crm.dar_name||'''''',''''CUSTOM_ROLE_SP'''');''
                ,''delete from HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.role_dar_mapping
                    where role_name=''''''||rdm.role_name||'''''' and dar_name=''''''||rdm.dar_name||'''''';'') ins_upd_qry
        from crm full outer join
        (   select * from HRDP_ADM_`+v_env+`_DB.DB_SCHEMA_SCH.role_dar_mapping 
            where sp_name=''CUSTOM_ROLE_SP'') rdm using (role_name,database_name,dar_name)
        where rdm.dar_name is null or crm.dar_name is null;`
    var list_grants_result=snowflake.createStatement( {sqlText: list_grants} ).execute();

    //Looping through the databases
    while (list_grants_result.next()) {
        var grnt_rvk=list_grants_result.getColumnValue(1);
        snowflake.createStatement( {sqlText: grnt_rvk} ).execute();
        var ins_upd=list_grants_result.getColumnValue(2);
        snowflake.createStatement( {sqlText: ins_upd} ).execute();
    }

    return ''Success'';

    ';"
    )
}}
select 1 as a
from dual
