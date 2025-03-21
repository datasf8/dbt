{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, startdate) as professional_field_id,
    externalcode as professional_field_code,
    startdate as professional_field_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as professional_field_end_date,
    name as professional_field_name_en,
    nvl(name_fr_fr, 'Not Translated FR') as professional_field_name_fr,
    status as professional_field_status
from {{ ref("stg_fo_job_function_flatten") }}
where dbt_valid_to is null
