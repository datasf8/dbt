{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    hc as (
        select
            user_id as hc_user_id,
            job_start_date as hc_job_start_date,
            job_end_date as hc_job_end_date,
            headcount_type_code as hc_headcount_type_code,
            headcount_present_flag as hc_headcount_present_flag
        from {{ ref("headcount_v1") }}
    ),
    job_info as (
        select
            user_id as ji_user_id,
            job_end_date as ji_job_end_date,
            job_start_date as ji_job_start_date,
            event_reasons_code as ji_event_reasons_code
        from {{ ref("job_information_v1") }}
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    reason as (
        select *
        from {{ ref("termination_event_reasons_v1") }}
        where termination_event_reasons_type_code = 'TER'
    ),
    jn_reason as (
        select *
        from reason
        inner join
            job_info on reason.event_reasons_code = job_info.ji_event_reasons_code
    ),
    hc_count as (
        select hcc_user_id, count(*) as cnt
        from
            (
                select
                    user_id as hcc_user_id,
                    job_start_date as hcc_job_start_date,
                    job_end_date as hcc_job_end_date,
                    headcount_type_code as hcc_headcount_type_code,
                    headcount_present_flag as hcc_headcount_present_flag

                from {{ ref("headcount_v1") }} hcc
                left outer join
                    jn_reason
                    on hcc.user_id = jn_reason.ji_user_id
                    and jn_reason.ji_job_start_date <= hcc.job_start_date
            )
        group by hcc_user_id
    ),
    jn_hc_greater3 as (
        select *
        from

            (
                select
                    hc_user_id,
                    ji_user_id,
                    ji_job_start_date,
                    ji_job_end_date,
                    hc_job_start_date,
                    hc_job_end_date,
                    hc_headcount_type_code,
                    hc_headcount_present_flag,

                    row_number() over (
                        partition by hc_user_id order by hc_job_start_date desc
                    ) as rn

                from jn_reason
                inner join hc_count on ji_user_id = hcc_user_id and cnt > 3
                left outer join
                    hc
                    on ji_user_id = hc_user_id
                    and hc_job_start_date <= ji_job_start_date
            -- and JI_JOB_START_DATE-5=hc_JOB_END_DATE -- to get the headcount type
            -- before his termination
            -- and JI_JOB_START_DATE=hc_JOB_START_DATE
            )
        where rn in (4, 5, 6)
    ),
    jn_hc_equal3 as (

        select
            hc_user_id,
            ji_user_id,
            ji_job_start_date,
            ji_job_end_date,
            hc_job_start_date,
            hc_job_end_date,
            hc_headcount_type_code,
            hc_headcount_present_flag,

            row_number() over (
                partition by hc_user_id, hc_job_start_date
                order by hc_job_start_date desc
            ) as rn

        from jn_reason
        inner join hc_count on ji_user_id = hcc_user_id and cnt = 3
        left outer join
            hc
            on ji_user_id = hc_user_id
            -- and JI_JOB_START_DATE-1=hc_JOB_END_DATE -- to get the headcount type
            -- before his termination
            and ji_job_start_date = hc_job_start_date

    )

select
    hc_user_id as user_id,
    ji_job_start_date as job_start_date,
    ji_job_end_date as job_end_date,
    hc_headcount_type_code as headcount_type_code,
    hc_headcount_present_flag as headcount_present_flag
from jn_hc_equal3
where hc_user_id is not null
union
select
    hc_user_id as user_id,
    ji_job_start_date as job_start_date,
    ji_job_end_date as job_end_date,
    hc_headcount_type_code as headcount_type_code,
    hc_headcount_present_flag as headcount_present_flag
from jn_hc_greater3
where hc_user_id is not null
