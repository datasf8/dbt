{% macro rls_hire() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% if execute %}
        {% set rRLS = run_query(
            "show row access policies like 'HIRE_RLS_POL' in schema cmn_core_sch"
        ) %}
        {% if rRLS | length > 0 %}
            {% set qDrop %}
                select 'ALTER TABLE '||ref_database_name||'.'||ref_schema_name||'.'||ref_entity_name
                        ||' DROP ROW ACCESS POLICY '||policy_db||'.'||policy_schema||'.'||policy_name||';' qDrop
                from table (information_schema.policy_references (policy_name => 'HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.HIRE_RLS_POL'))
            {% endset %}
            {% set rDrop = run_query(qDrop) %}
            {% if execute %} {% set rlDrop = rDrop.columns[0].values() %}
            {% else %} {% set rlDrop = [] %}
            {% endif %}
            {% for qlDrop in rlDrop %} {% do run_query(qlDrop) %} {% endfor %}
            {% do run_query(
                "drop row access policy if exists cmn_core_sch.hire_rls_pol"
            ) %}
        {% endif %}
    {% else %} {% set rRLS = [] %}
    {% endif %}

    {% set qRestAll %}
        create or replace row access policy cmn_core_sch.hire_rls_pol as (p_user_id varchar) returns boolean
        ->  exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                    where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
            or exists (select 1 from cmn_core_sch.rel_sf_group_rls_user a
                        left join cmn_core_sch.dim_param_security_replication r on copy_to_user = current_user
                        join cmn_core_sch.rel_hire_sf_group_rls_employee b 
                            on a.grp_id = b.reeu_group_id_reem
                            and a.employee_upn = collate(nvl(copy_from_user,current_user),'en-cs')
                            and b.reeu_employee_id_ddep = 'ALL')
            or exists (select 1 from cmn_core_sch.rel_sf_group_rls_user a
                        left join cmn_core_sch.dim_param_security_replication r on copy_to_user = current_user
                        join cmn_core_sch.rel_hire_sf_group_rls_employee b 
                            on a.grp_id = b.reeu_group_id_reem
                            and a.employee_upn = collate(nvl(copy_from_user,current_user),'en-cs')
                            and b.reeu_employee_id_ddep != 'ALL'
                            and b.reeu_employee_id_ddep = p_user_id)
        ;
        
        call cmn_core_sch.rls_policy_apply_all_sp('%','%');
        --call cmn_core_sch.rls_policy_apply_all_sp('HRDP_CORE_{{ env_var('DBT_REGION') }}_DB','CMN_CORE_SCH');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
