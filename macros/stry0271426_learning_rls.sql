{% macro stry0271426_learning_rls() %}
 
    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% if execute %}
        {% set rRLS = run_query('show row access policies in schema lrn_core_sch') %}
        {% if rRLS|length > 0 %}
            {% set qDrop %}
                select 'ALTER TABLE '||ref_database_name||'.'||ref_schema_name||'.'||ref_entity_name
                        ||' DROP ROW ACCESS POLICY '||policy_db||'.'||policy_schema||'.'||policy_name||';' qDrop
                from table (information_schema.policy_references (policy_name => 'HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.LRN_CORE_SCH.LRN_RLS_POL'))
            {% endset %}
            {% set rDrop = run_query(qDrop) %}
            {% if execute %}
                {% set rlDrop = rDrop.columns[0].values() %}
            {% else %}
                {% set rlDrop = [] %}
            {% endif %}
            {% for qlDrop in rlDrop %}
                {% do run_query(qlDrop) %}
            {% endfor %}
            {% do run_query('drop row access policy if exists lrn_core_sch.lrn_rls_pol') %}
        {% endif %}
    {% else %}
        {% set rRLS = [] %}
    {% endif %}

    {% set qRestAll %}
        create row access policy if not exists cmn_core_sch.lrn_rls_pol as (p_user_id varchar) returns boolean
        ->  exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                        where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
            or exists (select 1 from cmn_core_sch.rel_employee_user
                        where reeu_subject_domain_reeu = 'LRN' and reeu_access_employee_upn_ddep = current_user()
                            and reeu_employee_id_ddep = 'ALL')
            or exists (select 1 from cmn_core_sch.rel_employee_user
                        where reeu_subject_domain_reeu = 'LRN'and reeu_access_employee_upn_ddep = current_user()  
                            and reeu_employee_id_ddep != 'ALL' and reeu_employee_id_ddep = p_user_id)
        ;

        alter table CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS add column if not exists updated_at timestamp;
        alter table CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS add column if not exists updated_by varchar;

        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0271426';
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE','EMPLOYMENT_DETAILS','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE','EMPLOYMENT_TERMINATION_DETAILS','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE','JOB_INFORMATION','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H09_LOREAL_CV_EU_{{ env_var('DBT_REGION') }}_PRIVATE','AWARDS','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H09_LOREAL_CV_EU_{{ env_var('DBT_REGION') }}_PRIVATE','EDUCATION','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H09_LOREAL_CV_EU_{{ env_var('DBT_REGION') }}_PRIVATE','LANGUAGE_MASTERY','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H09_LOREAL_CV_EU_{{ env_var('DBT_REGION') }}_PRIVATE','PREVIOUS_EXPERIENCE','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H10_DOCUMENT_MANAGEMENT_EU_{{ env_var('DBT_REGION') }}_PRIVATE','DOCUMENT_ACKNOWLEDGMENT','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE','LEARNING_ACTIVITY','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE','EMPLOYEE_PROFILE_DIRECTORY','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE','USER_ASSIGNMENT_PROFILE','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE','USER_SECURITY_DOMAIN','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H08_LEARNING_EU_{{ env_var('DBT_REGION') }}_PRIVATE','USER_PROFILE_ROLE','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','LRN_PUB_SCH','FACT_TARGET_MAND_MUST_DETAILED_VW','LEARNER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','LRN_PUB_SCH','FACT_ASSIGNMENT_PROFILE_TARGET','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','ACK_PUB_SCH','FACT_DOCUMENT_ACKNOWLEDGMENT','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('LRN','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','DIM_EMPLOYEE_PROFILE_V1','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE','HEADCOUNT','USER_ID',current_timestamp(),'STRY0271426');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('EC','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE','HEADCOUNT_MONTHLY','USER_ID',current_timestamp(),'STRY0271426');
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');
        --call cmn_core_sch.rls_policy_apply_all_sp('HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','LRN_PUB_SCH');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
