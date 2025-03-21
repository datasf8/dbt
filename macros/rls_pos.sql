{% macro rls_pos() %}

-------------- Prerequisite to Set the Ground ---------
{% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
{% do run_query(qPrerequisite) %}

-----------------------Check existing Policy and drop from attached table -------------

 {% if execute %}
        {% set rRLS = run_query(
            "show row access policies like 'POS_RLS_POL' in schema cmn_core_sch"
        ) %}
        {% if rRLS | length > 0 %}
            {% set qDrop %}
                select 'ALTER TABLE '||ref_database_name||'.'||ref_schema_name||'.'||ref_entity_name
                        ||' DROP ROW ACCESS POLICY '||policy_db||'.'||policy_schema||'.'||policy_name||';' qDrop
                from table (information_schema.policy_references (policy_name => 'HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.POS_RLS_POL'))
            {% endset %}
            {% set rDrop = run_query(qDrop) %}
            {% if execute %} {% set rlDrop = rDrop.columns[0].values() %}
            {% else %} {% set rlDrop = [] %}
            {% endif %}
            {% for qlDrop in rlDrop %} {% do run_query(qlDrop) %} {% endfor %}
            {% do run_query(
                "drop row access policy if exists cmn_core_sch.pos_rls_pol"
            ) %}
        {% endif %}
    {% else %} {% set rRLS = [] %}
    {% endif %}

-------------- Recreating POS RLS Policy  ---------
{% set qRestAll %}
create or replace row access policy cmn_core_sch.pos_rls_pol as (p_bu varchar , p_spec varchar) returns boolean
-> exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                        where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
            or exists (select 1 from cmn_core_sch.rel_ec_bu_spec_by_user
                        where user_upn = nvl(
                                                                        (
                                                                        select copy_from_user
                                                                        from cmn_core_sch.dim_param_security_replication
                                                                        where copy_to_user = current_user
                                                                        ),
                                                                        current_user
                                                                        )  and business_unit_code = 'ALL')
            or exists (select 1 from cmn_core_sch.rel_ec_bu_spec_by_user
                        where user_upn = nvl(
                                                                        (
                                                                        select copy_from_user
                                                                        from cmn_core_sch.dim_param_security_replication
                                                                        where copy_to_user = current_user
                                                                        ),
                                                                        current_user
                                                                        )  and business_unit_code != 'ALL'
                            and business_unit_code = p_bu and specialization_code = p_spec)
                            ;
    {% endset %}
    {% do run_query(qRestAll) %}
{% endmacro %}
