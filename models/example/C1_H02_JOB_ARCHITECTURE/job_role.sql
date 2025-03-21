{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, startdate) as job_role_id,
    externalcode as job_role_code,
    startdate as job_role_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as job_role_end_date,
    name as job_role_name_en,
    nvl(name_fr_fr, 'Not Translated FR') as job_role_name_fr,
    cust_tospecialization_externalcode as specialization_code,
    s.specialization_id,
    status as job_role_status
from {{ ref("stg_fo_job_code_flatten") }} sfjcf
left join {{ ref('specialization') }} s
on sfjcf.cust_tospecialization_externalcode = s.specialization_code
and job_role_start_date between s.specialization_start_date and s.specialization_end_date
where sfjcf.dbt_valid_to is null