{{
    config(
        materialized="table",
        cluster_by=["reeu_group_id_reem"],
        transient=false,
        snowflake_warehouse="HRDP_DBT_PREM_WH",
    )
}}
 with 
    ec_rls_maxcount as (
        select count(distinct employee_id) as max_num_emp
        from {{ ref("dim_group_user") }} all_emp
        join {{ ref("dim_sf_roles_group") }} dsrg on all_emp.grp_id = dsrg.tgtgroup
        join
            {{ ref("dim_param_sf_roles") }} dpsr
            on dpsr.sfroleid = dsrg.sfroleid
            and dpsr.subjectdomain = 'CSRD'
    ),
    ec_rls as (
        select reeu_subject_domain_reeu, reeu_group_id_reem, reeu_employee_id_ddep
        from
            (
                with
                    employee_user as (
                        select
                            subjectdomain,
                            reeu_group_id_reem,
                            case
                                when max_num_emp = array_size(list_employees) or group_name = 'GRT_PPA_CSRD_ALL'
                                then 'ALL'::array
                                else list_employees
                            end as employee_id_list
                        from
                            (
                                select distinct
                                    subjectdomain,
                                    group_id as reeu_group_id_reem,
                                    group_name,
                                    max(max_num_emp) as max_num_emp,
                                    array_agg(distinct employee_id) as list_employees
                                from
                                    (
                                        select distinct
                                            dpsr.subjectdomain as subjectdomain,
                                            dgu_grt.grp_id as group_id,
                                            dgu_grt.groupname as group_name,
                                            dgu_tgt.employee_id as employee_id,
                                            max_num_emp
                                        from {{ ref("dim_param_sf_roles") }} dpsr
                                        join
                                            {{ ref("dim_sf_roles_group") }} dsrg
                                            on dpsr.sfroleid = dsrg.sfroleid
                                        join
                                            {{ ref("dim_group_user") }} dgu_grt
                                            on dgu_grt.grp_id = dsrg.grtgroup
                                            and dgu_grt.groupname != '$$EVERYONE$$'
                                        join
                                            {{ ref("dim_group_user") }} dgu_tgt
                                            on dgu_tgt.grp_id = dsrg.tgtgroup
                                        join ec_rls_maxcount on 1 = 1
                                        where subjectdomain = 'CSRD'

                                    ) a

                                group by 1, 2, 3
                            )
                    )

                select
                    subjectdomain as reeu_subject_domain_reeu,
                    reeu_group_id_reem as reeu_group_id_reem,

                    c.value::string as reeu_employee_id_ddep
                from employee_user, lateral flatten(input => employee_id_list) c
                order by reeu_group_id_reem, reeu_employee_id_ddep
            )
    ) 
 
select
    reeu_subject_domain_reeu,
    reeu_group_id_reem::number as reeu_group_id_reem,
    reeu_employee_id_ddep
from ec_rls
 