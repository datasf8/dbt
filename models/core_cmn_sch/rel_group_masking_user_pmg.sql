	{{ config(materialized="table", transient=false) }}

select distinct reeu_subject_domain_reeu, reeu_group_id_reem, reeu_employee_id_ddep
from {{ ref("rel_pmg_sf_group_rls_employee") }} a
join
    {{ ref('rel_sf_group_rls_user') }} b
    on a.reeu_group_id_reem = b.grp_id
join
    {{ env_var("DBT_PUB_DB") }}.cmn_pub_sch.dim_employee_profile_vw c
    on b.employee_id = c.employee_id
join
    {{ env_var("DBT_PUB_DB") }}.cmn_pub_sch.dim_employee_profile_vw d
    on a.reeu_employee_id_ddep = d.employee_id
where
    c.professional_field_code in ('PF000016')
    and d.professional_field_code in ('PF000016')