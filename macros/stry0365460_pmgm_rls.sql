{% macro stry0365460_pmgm_rls() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% if execute %}
        {% set rRLS = run_query(
            "show row access policies like 'PMGM_RLS_POL' in schema cmn_core_sch"
        ) %}
        {% if rRLS | length > 0 %}
            {% set qDrop %}
                select 'ALTER TABLE '||ref_database_name||'.'||ref_schema_name||'.'||ref_entity_name
                        ||' DROP ROW ACCESS POLICY '||policy_db||'.'||policy_schema||'.'||policy_name||';' qDrop
                from table (information_schema.policy_references (policy_name => 'HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.PMGM_RLS_POL'))
            {% endset %}
            {% set rDrop = run_query(qDrop) %}
            {% if execute %} {% set rlDrop = rDrop.columns[0].values() %}
            {% else %} {% set rlDrop = [] %}
            {% endif %}
            {% for qlDrop in rlDrop %} {% do run_query(qlDrop) %} {% endfor %}
            {% do run_query(
                "drop row access policy if exists cmn_core_sch.pmgm_rls_pol"
            ) %}
        {% endif %}
    {% else %} {% set rRLS = [] %}
    {% endif %}

    {% set qRestAll %}
        create row access policy if not exists cmn_core_sch.pmgm_rls_pol as (p_user_id varchar) returns boolean
        ->  exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                        where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
            or exists (select 1 from cmn_core_sch.rel_employee_user
                        where reeu_subject_domain_reeu = 'PMGM' and reeu_access_employee_upn_ddep = current_user()
                            and reeu_employee_id_ddep = 'ALL')
            or exists (select 1 from cmn_core_sch.rel_employee_user
                        where reeu_subject_domain_reeu = 'PMGM'and reeu_access_employee_upn_ddep = current_user()  
                            and reeu_employee_id_ddep != 'ALL' and reeu_employee_id_ddep = p_user_id)
        ;

        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0365460';
        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where DPRS_TABLE_DPRS in ('DOCUMENT_ACKNOWLEDGMENT','FACT_DOCUMENT_ACKNOWLEDGMENT');

        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('PMGM','HRDP_CORE_{{ env_var('DBT_REGION') }}_DB','CMN_CORE_SCH','DIM_EMPLOYEE_PROFILE_FINAL','DDEP_EMPLOYEE_ID_DDEP',current_timestamp(),'STRY0365460');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('PMGM','HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB','BTDP_DS_C2_H10_DOCUMENT_MANAGEMENT_EU_{{ env_var('DBT_REGION') }}_PRIVATE','DOCUMENT_ACKNOWLEDGMENT','USER_ID',current_timestamp(),'STRY0365460');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('PMGM','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','ACK_PUB_SCH','FACT_DOCUMENT_ACKNOWLEDGMENT','USER_ID',current_timestamp(),'STRY0365460');
        
        ALTER TABLE HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C2_H10_DOCUMENT_MANAGEMENT_EU_{{ env_var('DBT_REGION') }}_PRIVATE.DOCUMENT_ACKNOWLEDGMENT DROP ALL ROW ACCESS POLICIES;
        ALTER TABLE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.ACK_PUB_SCH.FACT_DOCUMENT_ACKNOWLEDGMENT DROP ALL ROW ACCESS POLICIES;
        
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');
        call cmn_core_sch.rls_policy_apply_all_sp('HRDP_CORE_{{ env_var('DBT_REGION') }}_DB','CMN_CORE_SCH');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
