{{
    config(
        materialized="table",
        cluster_by=["reur_group_id_ddep"],
        transient=false,
        snowflake_warehouse="HRDP_DBT_PREM_WH",
    )
}}

with
    max_emp as (
        select count(distinct employee_id) as max_num_emp
        from {{ ref("dim_group_user") }}
    ),
    masking as (

        select
            ddep_group_id_ddep,
            dprt_tag_value_dprt,
            case
                when max_num_emp = array_size(list_employees)
                then 'ALL'::array
                else list_employees
            end as access_employee_id

        from
            (

                select distinct
                    dgu_grt.grp_id as ddep_group_id_ddep,
                    dptc.dprt_tag_value_dprt,
                    array_agg(distinct dgu_tgt.employee_id) as list_employees,
                    max(max_num_emp) as max_num_emp
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
                    and usr.disabled = false,
                    max_emp
                group by 1, 2
            ) agg

    )

select
    ddep_group_id_ddep::number as reur_group_id_ddep,
    c.value::string as reur_employee_id_ddep,
    dprt_tag_value_dprt as reur_tag_value_dptc
from masking, lateral flatten(input => access_employee_id) c
order by 3, 1
