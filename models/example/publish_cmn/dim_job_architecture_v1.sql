{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    professional_field_v1 as (
        select *
        from {{ ref("professional_field_v1") }}
        where professional_field_start_date <= current_date()
        qualify
            row_number() over (
                partition by professional_field_code
                order by professional_field_start_date desc
            )
            = 1
    )
select
    hash(jr.job_role_code, jr.job_role_start_date) as job_architecture_sk,
    jr.job_role_id,
    jr.job_role_code,
    jr.job_role_start_date as job_architecture_start_date,
    jr.job_role_end_date as job_architecture_end_date,
    jr.job_role_name_en,
    jr.job_role_name_en || ' (' || jr.job_role_code || ')' as job_role_name_label,
    jr.job_role_status,
    sp.specialization_id,
    jr.specialization_code,
    sp.specialization_name_en,
    sp.specialization_name_en
    || ' ('
    || jr.specialization_code
    || ')' as specialization_name_label,
    sp.specialization_status,
    pf.professional_field_id,
    sp.professional_field_code,
    pf.professional_field_name_en,
    pf.professional_field_name_en
    || ' ('
    || sp.professional_field_code
    || ')' as professional_field_name_label,
    pf.professional_field_status
from {{ ref("job_role_v1") }} jr
left join
    {{ ref("specialization_v1") }} sp
    on jr.specialization_code = sp.specialization_code
    and job_role_start_date
    between specialization_start_date and specialization_end_date
left join
    professional_field_v1 pf on sp.professional_field_code = pf.professional_field_code
