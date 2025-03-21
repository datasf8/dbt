{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    ji_rsn1 as (
        select
            user_id,
            job_end_date,
            job_start_date,
            ji.event_reasons_code,
            rsn.event_reasons_type_code
        from {{ ref("job_information_v1") }} ji
        join
            {{ ref("event_reasons_categories_v1") }} rsn
            on ji.event_reasons_code = rsn.event_reasons_code
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    ji_rsn2 as (
        select
            *,
            lag(event_reasons_type_code) over (
                partition by user_id order by job_start_date
            ) prev_event_reasons_type_code
        from ji_rsn1
    ),
    hc as (
        select
            *,
            lag(headcount_line_id) over (
                partition by user_id, headcount_type_code order by job_start_date
            ) previous_headcount_line_id
        from {{ ref("headcount_v1") }}
    ),
    hc_ji_rsn as (
        select hc.*, jir.event_reasons_code, jir.event_reasons_type_code
        from hc
        join
            ji_rsn2 jir
            on hc.user_id = jir.user_id
            and hc.job_start_date = jir.job_start_date
            and (
                jir.event_reasons_type_code != 'TER'
                or (
                    jir.event_reasons_type_code = 'TER'
                    and nvl(jir.prev_event_reasons_type_code, '') != 'TER'
                )
            )
    )
select
    user_id,
    headcount_line_id,
    previous_headcount_line_id,
    case
        when event_reasons_type_code = 'TER'
        then 'TERMINATION'
        when event_reasons_type_code = 'HIR'
        then 'HIRING'
        when event_reasons_type_code = 'MOB'
        then 'MOBILITY'
        else event_reasons_type_code
    end as mobility_type,
    event_reasons_code,
    job_start_date as mobility_date
from hc_ji_rsn
