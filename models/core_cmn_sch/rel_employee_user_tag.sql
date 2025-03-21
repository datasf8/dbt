{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        snowflake_warehouse="HRDP_DBT_PREM_WH",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;call cmn_core_sch.security_replication_sp();",
    )
}}
with
    max_emp as (
        select count(distinct employee_id) as max_num_emp
        from {{ ref("dim_group_user") }}
    ),
    masking as (
        select ddep_employee_upn_ddep, access_employee_id, dprt_tag_value_dprt
        from
            (
                select distinct
                    ep.ddep_employee_upn_ddep,
                    dgu_tgt.employee_id as access_employee_id,
                    dptc.dprt_tag_value_dprt
                from {{ ref("dim_sf_roles_group") }} dsrg
                join
                    {{ ref("dim_param_roles_tags") }} dptc
                    on dsrg.role_name = dptc.dprt_role_name_dprt
                join
                    {{ ref("dim_group_user") }} dgu_grt
                    on dgu_grt.grp_id = dsrg.grtgroup
                    and dgu_grt.groupname != '$$EVERYONE$$'
                join
                    {{ ref("dim_group_user") }} dgu_tgt
                    on dgu_tgt.grp_id = dsrg.tgtgroup
                join  /* User should be coming from employee profile source */
                    (
                        select
                            ddep_employee_id_ddep,
                            upper(ddep_employee_upn_ddep) ddep_employee_upn_ddep
                        from {{ ref("dim_employee_profile") }}
                        where ddep_status_ddep = 't'
                    ) ep
                    on dgu_grt.employee_id = ep.ddep_employee_id_ddep
                join  /* User should have Snowflake Access */
                    snowflake.account_usage.users usr
                    on ep.ddep_employee_upn_ddep = usr.login_name
                    and usr.deleted_on is null
                    and usr.disabled = false
                where  /* User should have RLS Access */
                    exists (
                        select 1
                        from {{ ref("rel_employee_user") }} reu
                        where dgu_grt.employee_id = reu.reeu_access_employee_id_reem
                    )
            )
    )
select
    ddep_employee_upn_ddep as reur_access_employee_upn_ddep,
    access_employee_id as reur_employee_id_ddep,
    dprt_tag_value_dprt as reur_tag_value_dptc
from masking
order by 3, 1
