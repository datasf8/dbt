{{
    config(
        materialized="table",
        cluster_by=["reeu_access_employee_upn_ddep"],
        transient=false,
        snowflake_warehouse="HRDP_DBT_PREM_WH",
    )
}}

with
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
    )

select
     reeu_subject_domain_reeu,
     max(reeu_access_employee_id_reem) as reeu_access_employee_id_reem,
     reeu_access_employee_upn_ddep,
     LPAD(reeu_employee_id_ddep,8,'0') as reeu_employee_id_ddep
from lrn_rls
group by all
union all
select 'LRN', '00133986', 'FREDERIC.EQUILBEC@LOREAL.COM', 'ALL'
