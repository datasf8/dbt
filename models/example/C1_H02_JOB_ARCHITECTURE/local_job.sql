{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    us_jobcode as (

        select
            hash(externalcode, effectivestartdate) as local_job_id,
            externalcode as local_job_code,
            effectivestartdate as local_job_start_date,
            iff(
                mdfsystemeffectiveenddate in ('9999-12-31'),
                lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
                    partition by externalcode order by effectivestartdate
                ),
                mdfsystemeffectiveenddate
            ) as local_job_end_date,
            externalname_defaultvalue as local_job_name_en,
            nvl(externalname_fr_fr, 'Not Translated FR') as local_job_name_fr,
            cust_globaljobcode as job_role_code,
            cust_country as country_code,
            mdfsystemstatus as local_job_status
        from {{ ref("stg_cust_usjobcode_flatten") }}
        where dbt_valid_to is null
    )

select us_jobcode.*, job_role_id, ctry.country_id as local_job_country_id
from us_jobcode
left outer join
    {{ ref("job_role_v2") }} jobrole
    on jobrole.job_role_code = us_jobcode.job_role_code
    and us_jobcode.local_job_start_date <= jobrole.job_role_end_date
    and us_jobcode.local_job_end_date >= jobrole.job_role_start_date
left outer join
    {{ ref("country_v1") }} ctry
    on ctry.country_code = us_jobcode.country_code
    and us_jobcode.local_job_start_date <= ctry.country_end_date
    and us_jobcode.local_job_end_date >= ctry.country_start_date

qualify
    row_number() over (
        partition by local_job_id
        order by ctry.country_start_date desc, jobrole.job_role_start_date desc
    )
    = 1
