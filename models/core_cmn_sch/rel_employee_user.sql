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
-- pmgm
with
    pmgm_rls as (
        select
            reeu_subject_domain_reeu,
            reeu_access_employee_id_reem,
            reeu_access_employee_upn_ddep,
            reeu_employee_id_ddep
        from
            (
                with
                    employee_user as (
                        select
                            subjectdomain,
                            access_employee_id,
                            access_employee_upn,
                            case
                                when max_num_emp = array_size(list_employees)
                                then 'ALL'::array
                                else list_employees
                            end as employee_id_list
                        from
                            (
                                select distinct
                                    subjectdomain,
                                    access_employee_id,
                                    c.login_name as access_employee_upn,
                                    max(max_num_emp) as max_num_emp,
                                    array_agg(distinct employee_id) as list_employees
                                from
                                    (
                                        select distinct
                                            dpsr.subjectdomain as subjectdomain,
                                            dgu_grt.employee_id as access_employee_id,
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
                                        join
                                            (
                                                select
                                                    count(
                                                        distinct employee_id
                                                    ) as max_num_emp
                                                from {{ ref("dim_group_user") }} all_emp
                                            )
                                            on 1 = 1
                                        where subjectdomain = 'PMGM'
                                        union all
                                        select distinct
                                            'PMGM' as subjectdomain,
                                            rem.reem_fk_manager_key_ddep
                                            as access_employee_id,
                                            dep.ddep_employee_id_ddep as employee_id,
                                            max_num_emp
                                        from {{ ref("rel_employee_managers") }} rem
                                        join
                                            {{ ref("dim_employee_profile") }} dep
                                            on dep.ddep_employee_id_ddep
                                            = rem.reem_employee_profile_key_ddep
                                        join
                                            (
                                                select
                                                    count(
                                                        distinct employee_id
                                                    ) as max_num_emp
                                                from {{ ref("dim_group_user") }} all_emp
                                            )
                                            on 1 = 1
                                    ) a
                                join
                                    {{ ref("dim_employee_profile") }} b
                                    on a.access_employee_id = b.ddep_employee_id_ddep
                                    and b.ddep_status_ddep = 't'
                                join
                                    snowflake.account_usage.users c
                                    on upper(b.ddep_employee_upn_ddep)
                                    = upper(c.login_name)
                                where c.deleted_on is null and c.disabled = false
                                group by 1, 2, 3
                            )
                    )
                select
                    subjectdomain as reeu_subject_domain_reeu,
                    access_employee_id as reeu_access_employee_id_reem,
                    access_employee_upn as reeu_access_employee_upn_ddep,
                    c.value::string as reeu_employee_id_ddep
                from employee_user, lateral flatten(input => employee_id_list) c
                order by reeu_access_employee_id_reem, reeu_employee_id_ddep
            )
    ),  -- lrn rls
    lrn_rls_maxcount as (
        select count(distinct user_id) as max_num_emp
        from
            {{ env_var("DBT_SDDS_DB") }}.{{ env_var("DBT_C2_H08_LEARNING") }}.user_security_domain_v1
    ),
    lrn_rls as (
        select
            reeu_subject_domain_reeu,
            reeu_access_employee_id_reem,
            reeu_access_employee_upn_ddep,
            reeu_employee_id_ddep
        from
            (
                with
                    employee_user as (
                        select
                            subjectdomain,
                            access_employee_id,
                            access_employee_upn,
                            case
                                when max_num_emp = array_size(list_employees)
                                then 'ALL'::array
                                else list_employees
                            end as employee_id_list
                        from
                            (
                                select distinct
                                    'LRN' as subjectdomain,
                                    ur.user_id as access_employee_id,
                                    u.login_name as access_employee_upn,
                                    array_agg(distinct usd.user_id) as list_employees
                                from
                                    {{ env_var("DBT_SDDS_DB") }}.
                                    {{ env_var("DBT_C2_H08_LEARNING") }}.user_profile_role_v1 ur
                                join
                                    {{ env_var("DBT_SDDS_DB") }}.
                                    {{ env_var("DBT_C1_H08_LEARNING") }}.roles_domain_restriction_access_v1 dra
                                    on ur.role_code = dra.role_code
                                join
                                    {{ env_var("DBT_SDDS_DB") }}.
                                    {{ env_var("DBT_C1_H08_LEARNING") }}.domain_restrictions_v1 dr
                                    on dra.domain_restriction_code
                                    = dr.domain_restriction_code
                                join
                                    {{ env_var("DBT_SDDS_DB") }}.
                                    {{ env_var("DBT_C2_H08_LEARNING") }}.user_security_domain_v1 usd
                                    on dr.security_domain_code
                                    = usd.security_domain_code
                                    and user_security_domain_end_date is null
                                join
                                    {{ env_var("DBT_SDDS_DB") }}.
                                    {{ env_var("DBT_C2_H05_EMPLOYMENT_INFORMATION") }}.employee_profile_directory_v1 epd
                                    on ur.user_id = epd.user_id
                                    and epd.ep_employee_status = 't'
                                join
                                    snowflake.account_usage.users u
                                    on upper(epd.username) = upper(u.login_name)
                                    and u.deleted_on is null
                                    and u.disabled = false
                                group by 1, 2, 3
                            )
                        join lrn_rls_maxcount on 1 = 1
                    )
                select
                    subjectdomain as reeu_subject_domain_reeu,
                    access_employee_id as reeu_access_employee_id_reem,
                    access_employee_upn as reeu_access_employee_upn_ddep,
                    c.value::string as reeu_employee_id_ddep
                from employee_user, lateral flatten(input => employee_id_list) c
                order by reeu_access_employee_id_reem, reeu_employee_id_ddep
            )
    ),  -- ec rls
    ec_rls_maxcount as (
        select count(distinct employee_id) as max_num_emp
        from {{ ref("dim_group_user") }} all_emp
        join {{ ref("dim_sf_roles_group") }} dsrg on all_emp.grp_id = dsrg.tgtgroup
        join
            {{ ref("dim_param_sf_roles") }} dpsr
            on dpsr.sfroleid = dsrg.sfroleid
            and dpsr.subjectdomain = 'EC'
    ),
    ec_rls as (
        select
            reeu_subject_domain_reeu,
            reeu_access_employee_id_reem,
            reeu_access_employee_upn_ddep,
            reeu_employee_id_ddep
        from
            (
                with
                    employee_user as (
                        select
                            subjectdomain,
                            access_employee_id,
                            access_employee_upn,
                            case
                                when max_num_emp = array_size(list_employees)
                                then 'ALL'::array
                                else list_employees
                            end as employee_id_list
                        from
                            (
                                select distinct
                                    subjectdomain,
                                    access_employee_id,
                                    c.login_name as access_employee_upn,
                                    max(max_num_emp) as max_num_emp,
                                    array_agg(distinct employee_id) as list_employees
                                from
                                    (
                                        select distinct
                                            dpsr.subjectdomain as subjectdomain,
                                            dgu_grt.employee_id as access_employee_id,
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
                                        where subjectdomain = 'EC'
                                        union all
                                        select distinct
                                            'EC' as subjectdomaindomain,
                                            rem.manager_id as access_employee_id,
                                            dep.ddep_employee_id_ddep as employee_id,
                                            max_num_emp
                                        from
                                            {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.rel_ec_employee_manager rem
                                        join
                                            {{ ref("dim_employee_profile") }} dep
                                            on dep.ddep_employee_id_ddep
                                            = rem.employee_id
                                        join ec_rls_maxcount on 1 = 1
                                    ) a
                                join
                                    {{ ref("dim_employee_profile") }} b
                                    on a.access_employee_id = b.ddep_employee_id_ddep
                                    and b.ddep_status_ddep = 't'
                                join
                                    snowflake.account_usage.users c
                                    on upper(b.ddep_employee_upn_ddep)
                                    = upper(c.login_name)
                                where c.deleted_on is null and c.disabled = false
                                group by 1, 2, 3
                            )
                    )

                select
                    subjectdomain as reeu_subject_domain_reeu,
                    access_employee_id as reeu_access_employee_id_reem,
                    access_employee_upn as reeu_access_employee_upn_ddep,
                    c.value::string as reeu_employee_id_ddep
                from employee_user, lateral flatten(input => employee_id_list) c
                order by reeu_access_employee_id_reem, reeu_employee_id_ddep
            )
    ),  -- CMP rls
    cmp_rls_maxcount as (
        select count(distinct employee_id) as max_num_emp
        from {{ ref("dim_group_user") }} all_emp
        join {{ ref("dim_sf_roles_group") }} dsrg on all_emp.grp_id = dsrg.tgtgroup
        join
            {{ ref("dim_param_sf_roles") }} dpsr
            on dpsr.sfroleid = dsrg.sfroleid
            and dpsr.subjectdomain = 'CMP'
    ),
    cmp_rls as (
        select
            reeu_subject_domain_reeu,
            reeu_access_employee_id_reem,
            reeu_access_employee_upn_ddep,
            reeu_employee_id_ddep
        from
            (
                with
                    employee_user as (
                        select
                            subjectdomain,
                            access_employee_id,
                            access_employee_upn,
                            case
                                when max_num_emp = array_size(list_employees)
                                then 'ALL'::array
                                else list_employees
                            end as employee_id_list
                        from
                            (
                                select distinct
                                    subjectdomain,
                                    access_employee_id,
                                    c.login_name as access_employee_upn,
                                    max(max_num_emp) as max_num_emp,
                                    array_agg(distinct employee_id) as list_employees
                                from
                                    (
                                        select distinct
                                            dpsr.subjectdomain as subjectdomain,
                                            dgu_grt.employee_id as access_employee_id,
                                            dgu_tgt.employee_id as employee_id,
                                            max_num_emp
                                        from {{ ref("dim_param_sf_roles") }} dpsr
                                        join
                                            {{ ref("dim_sf_roles_group") }} dsrg
                                            on dpsr.sfroleid = dsrg.sfroleid
                                            and dpsr.subjectdomain = 'CMP'
                                        join
                                            {{ ref("dim_group_user") }} dgu_grt
                                            on dgu_grt.grp_id = dsrg.grtgroup
                                            and dgu_grt.groupname != '$$EVERYONE$$'
                                        join
                                            {{ ref("dim_group_user") }} dgu_tgt
                                            on dgu_tgt.grp_id = dsrg.tgtgroup
                                        join cmp_rls_maxcount on 1 = 1
                                        union all
                                        select distinct
                                            'CMP' as subjectdomaindomain,
                                            demh.manager_id as access_employee_id,
                                            dep.ddep_employee_id_ddep as employee_id,
                                            max_num_emp
                                        from
                                            {{ env_var("DBT_CORE_DB") }}.cmp_core_sch.dim_employee_manager_hr demh
                                        join
                                            {{ ref("dim_employee_profile") }} dep
                                            on dep.ddep_employee_id_ddep
                                            = demh.employee_id
                                        join cmp_rls_maxcount on 1 = 1
                                    ) a
                                join
                                    {{ ref("dim_employee_profile") }} b
                                    on a.access_employee_id = b.ddep_employee_id_ddep
                                    and b.ddep_status_ddep = 't'
                                join
                                    snowflake.account_usage.users c
                                    on upper(b.ddep_employee_upn_ddep)
                                    = upper(c.login_name)
                                where c.deleted_on is null and c.disabled = false
                                group by 1, 2, 3
                            )
                    )

                select
                    subjectdomain as reeu_subject_domain_reeu,
                    access_employee_id as reeu_access_employee_id_reem,
                    access_employee_upn as reeu_access_employee_upn_ddep,
                    c.value::string as reeu_employee_id_ddep
                from employee_user, lateral flatten(input => employee_id_list) c
                order by reeu_access_employee_id_reem, reeu_employee_id_ddep
            )
    )

select
    reeu_subject_domain_reeu,
    reeu_access_employee_id_reem,
    reeu_access_employee_upn_ddep,
    reeu_employee_id_ddep
from pmgm_rls
union all
select
    reeu_subject_domain_reeu,
    reeu_access_employee_id_reem,
    reeu_access_employee_upn_ddep,
    reeu_employee_id_ddep
from lrn_rls
union all
select
    reeu_subject_domain_reeu,
    reeu_access_employee_id_reem,
    reeu_access_employee_upn_ddep,
    reeu_employee_id_ddep
from ec_rls
union all
select
    reeu_subject_domain_reeu,
    reeu_access_employee_id_reem,
    reeu_access_employee_upn_ddep,
    reeu_employee_id_ddep
from cmp_rls
union all
select 'LRN', '00133986', 'FREDERIC.EQUILBEC@LOREAL.COM', 'ALL'
