{% macro rls() %}

-------------- Prerequisite to Set the Ground ---------
USE ROLE HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
USE DATABASE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;

-------------- One time Process: Table Creation with Cluster By ---------
create temporary table CMN_CORE_SCH.REL_EMPLOYEE_USER_temp as 
    select * from CMN_CORE_SCH.REL_EMPLOYEE_USER;
create or replace TABLE CMN_CORE_SCH.REL_EMPLOYEE_USER (
	REEU_SUBJECT_DOMAIN_REEU VARCHAR(200),
	REEU_ACCESS_EMPLOYEE_ID_REEM VARCHAR(16777216),
	REEU_ACCESS_EMPLOYEE_UPN_DDEP VARCHAR(16777216),
	REEU_EMPLOYEE_ID_DDEP VARCHAR(16777216)
) cluster by (reeu_subject_domain_reeu,reeu_access_employee_upn_ddep);
insert into CMN_CORE_SCH.REL_EMPLOYEE_USER select * from CMN_CORE_SCH.REL_EMPLOYEE_USER_temp;
create temporary table CMN_CORE_SCH.REL_EMPLOYEE_USER_TAG_temp as 
    select * from CMN_CORE_SCH.REL_EMPLOYEE_USER_TAG;
create or replace TABLE CMN_CORE_SCH.REL_EMPLOYEE_USER_TAG (
	REUR_ACCESS_EMPLOYEE_UPN_DDEP VARCHAR(16777216),
	REUR_EMPLOYEE_ID_DDEP VARCHAR(16777216),
	REUR_TAG_VALUE_DPTC VARCHAR(16777216)
) cluster by (REUR_TAG_VALUE_DPTC,REUR_ACCESS_EMPLOYEE_UPN_DDEP);
insert into CMN_CORE_SCH.REL_EMPLOYEE_USER_TAG select * from CMN_CORE_SCH.REL_EMPLOYEE_USER_TAG_temp;
-------------- Dropping CMP RLS Before modifying the RLS Policy  ---------

ALTER TABLE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMP_PUB_SCH.FACT_COMP_YER_TRANSACT_SNAPSHOT 
    DROP ROW ACCESS POLICY CMN_CORE_SCH.CMP_RLS_POL;
ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMP_CORE_SCH.FACT_COMP_YER_TRANSACT 
    DROP ROW ACCESS POLICY CMN_CORE_SCH.CMP_RLS_POL;
ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMP_CORE_SCH.FACT_VARIABLE_PAY 
    DROP ROW ACCESS POLICY CMN_CORE_SCH.CMP_RLS_POL;
ALTER TABLE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMP_PUB_SCH.FACT_VARIABLE_PAY_BY_GOALS_SNAPSHOT 
    DROP ROW ACCESS POLICY CMN_CORE_SCH.CMP_RLS_POL;
ALTER TABLE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMP_PUB_SCH.FACT_VARIABLE_PAY_SNAPSHOT 
    DROP ROW ACCESS POLICY CMN_CORE_SCH.CMP_RLS_POL;

-------------- Recreating CMP RLS Policy  ---------

create or replace row access policy cmn_core_sch.cmp_rls_pol as (p_user_id varchar) returns boolean
->  exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
	or exists (select 1 from cmn_core_sch.rel_employee_user
                where reeu_subject_domain_reeu = 'CMP' and reeu_access_employee_upn_ddep = current_user()
                    and reeu_employee_id_ddep = 'ALL')
    or exists (select 1 from cmn_core_sch.rel_employee_user
                where reeu_subject_domain_reeu = 'CMP' and reeu_access_employee_upn_ddep = current_user()
                    and reeu_employee_id_ddep != 'ALL' and reeu_employee_id_ddep = p_user_id)
;

-------------- Dropping EC RLS Before modifying the RLS Policy  ---------

ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.FACT_HEADCOUNT
    DROP ROW ACCESS POLICY CMN_CORE_SCH.EC_RLS_POL;
ALTER TABLE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_HEADCOUNT_SNAPSHOT
    DROP ROW ACCESS POLICY CMN_CORE_SCH.EC_RLS_POL;

-------------- Recreating EC RLS Policy  ---------

create or replace row access policy cmn_core_sch.ec_rls_pol as (p_user_id varchar) returns boolean
->  exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
	or exists (select 1 from cmn_core_sch.rel_employee_user
                where reeu_subject_domain_reeu = 'EC' and reeu_access_employee_upn_ddep = current_user()
                    and reeu_employee_id_ddep = 'ALL')
    or exists (select 1 from cmn_core_sch.rel_employee_user
                where reeu_subject_domain_reeu = 'EC' and reeu_access_employee_upn_ddep = current_user()
                    and reeu_employee_id_ddep != 'ALL' and reeu_employee_id_ddep = p_user_id)
;

/*
-------------- Dropping PM RLS Before modifying the RLS Policy  ---------
ALTER TABLE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_POSITION_MANAGEMENT_SNAPSHOT
    DROP ROW ACCESS POLICY CMN_CORE_SCH.PM_RLS_POL;
ALTER TABLE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.FACT_HEADCOUNT_SNAPSHOT_PM_VW
    DROP ROW ACCESS POLICY CMN_CORE_SCH.PM_RLS_POL;

-------------- Recreating EC RLS Policy  ---------
create or replace row access policy cmn_core_sch.pm_rls_pol as (p_user_id varchar) returns boolean
->  exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
    or exists (select 1 from cmn_core_sch.dim_position_access
                where access_employee_upn = current_user()
                    and scope_type='ST000004')
    or exists (select 1 from cmn_core_sch.dim_position_access
                where access_employee_upn = current_user()
                    and scope_type='ST000002' and scope_access=v_zone)
    or exists (select 1 from cmn_core_sch.dim_position_access
                where access_employee_upn = current_user()
                    and scope_type='ST000016' and scope_access=v_country)
;
/**/

-------------- Calling this will recreate the RLS Policies  ---------

call cmn_core_sch.rls_policy_apply_all_sp('%','%');

{% endmacro %}