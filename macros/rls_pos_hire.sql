{% macro rls_pos_hire() %}

{% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
{% endset %}
{% do run_query(qPrerequisite) %}


{% set qRestAll %}
        create or replace row access policy cmn_core_sch.pos_hire_rls_pol as (p_bu varchar,p_spec varchar) returns boolean
        -> exists (select 1 from cmn_core_sch.dim_param_snowflake_roles 
                        where dpsr_snowflake_role_dpsr=current_role() and dpsr_exclude_security_dpsr='Y')
            or exists (select 1 from cmn_core_sch.rel_hire_bu_spec_by_user
                        where user_upn = nvl(
                                                                        (
                                                                        select copy_from_user
                                                                        from cmn_core_sch.dim_param_security_replication
                                                                        where copy_to_user = current_user
                                                                        ),
                                                                        current_user
                                                                        )  and business_unit_code = 'ALL')
            or exists (select 1 from cmn_core_sch.rel_hire_bu_spec_by_user
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
        
        call cmn_core_sch.rls_policy_apply_all_sp('%','%');
        --call cmn_core_sch.rls_policy_apply_all_sp('HRDP_CORE_{{ env_var('DBT_REGION') }}_DB','CMN_CORE_SCH');

    {% endset %}
{% do run_query(qRestAll) %}

{% endmacro %}
