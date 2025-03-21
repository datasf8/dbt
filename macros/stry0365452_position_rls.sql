{% macro stry0365452_position_rls() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% if execute %}
        {% set rRLS = run_query(
            "show row access policies like 'POS_RLS_POL' in schema cmn_core_sch"
        ) %}
        {% if rRLS | length > 0 %}
            {% set qDrop %}
                select ('ALTER TABLE '||ref_database_name||'.'||ref_schema_name||'.'||ref_entity_name||' DROP ALL ROW ACCESS POLICIES;') qDrop
                from table (information_schema.policy_references (policy_name => 'HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.POS_RLS_POL'))
            {% endset %}
            {% set rDrop = run_query(qDrop) %}
            {% if execute %} {% set rlDrop = rDrop.columns[0].values() %}
            {% else %} {% set rlDrop = [] %}
            {% endif %}
            {% for qlDrop in rlDrop %} {% do run_query(qlDrop) %} {% endfor %}
        {% endif %}
    {% endif %}

    {% set qRestAll %}
        create or replace row access policy cmn_core_sch.pos_rls_pol as (p_bu varchar,p_spec varchar) returns boolean
        ->  exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                        where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
            or exists (select 1 from cmn_core_sch.rel_bu_spec_by_user
                        where user_upn = current_user() and business_unit_code = 'ALL')
            or exists (select 1 from cmn_core_sch.rel_bu_spec_by_user
                        where user_upn = current_user() and business_unit_code != 'ALL'
                            and business_unit_code = p_bu and specialization_code = p_spec)
        ;

        delete from CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS where updated_by='STRY0365452';
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('POS','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_POSITION_MANAGEMENT_V1','BUSINESS_UNIT_CODE,SPECIALIZATION_CODE',current_timestamp(),'STRY0365452');
        insert into CMN_CORE_SCH.DIM_PARAM_RLS_COLUMNS values('POS','HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH','FACT_POSITION','BUSINESS_UNIT_CODE,SPECIALIZATION_CODE',current_timestamp(),'STRY0365452');
        --call cmn_core_sch.rls_policy_apply_all_sp('%','%');
        call cmn_core_sch.rls_policy_apply_all_sp('HRDP_PUB_{{ env_var('DBT_REGION') }}_DB','CMN_PUB_SCH');

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
