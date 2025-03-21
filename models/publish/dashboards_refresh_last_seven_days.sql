{{ config(materialized="table") }}
select 
-- c.*,
distinct
 a.*, b.last_run_time
from
    (
        select job_name, last_success_end_time, last_success_start_time
        from
            (
                select
                    job_name,
                    last_run_timestamp as last_success_start_timestamp,
                    last_success_end_time,
                    last_success_start_time,
                    row_number() over (
                        partition by job_name order by last_success_start_timestamp desc
                    ) as rnw
                from
                    (
                        select *
                        from
                            (
                                select
                                    job_name,
                                    date_trunc(
                                        day, to_timestamp(jst)
                                    ) as last_run_timestamp,
                                    cast(st as integer) as status,
                                    last_success_end_time,
                                    last_success_start_time
                                from
                                    (
                                        select
                                            *,
                                            case
                                                when job_execution_status = 'success'
                                                then 1
                                                else 0
                                            end as st
                                        from
                                            (
                                                select
                                                    *,
                                                    row_number() over (
                                                        partition by
                                                            job_name,
                                                            date_trunc(day, jst)
                                                        order by rnk1 desc
                                                    ) as max_2
                                                from
                                                    (
                                                        select
                                                            job_name,
                                                            job_start_time as jst,
                                                            job_execution_status,
                                                            rank() over (
                                                                partition by
                                                                    job_name,
                                                                    date_trunc(
                                                                        day,
                                                                        job_start_time
                                                                    )
                                                                order by
                                                                    job_start_time asc
                                                            ) as rnk1,
                                                            max(
                                                                job_end_time
                                                            ) as last_success_end_time,
                                                            max(
                                                                job_start_time
                                                            ) as last_success_start_time
                                                        from
                                                            hrdp_stg_{{ env_var('DBT_REGION') }}_db.cmn_stg_sch.stg_job_execution_log
                                                        where
                                                            job_execution_status
                                                            = 'success'
                                                            and job_start_time
                                                            >= to_date(
                                                                dateadd(
                                                                    day,
                                                                    -7,
                                                                    date_trunc(
                                                                        'day',
                                                                        current_date()
                                                                    )
                                                                )
                                                            )
                                                        group by 1, 2, 3
                                                        order by job_start_time desc
                                                    ) a
                                            )
                                        where max_2 = 1
                                    )
                            )
                        where status = 1
                    )
                qualify rnw = 1
            )
    ) a inner join 
    (
        select job_name, max(job_end_time) as last_run_time
        from hrdp_stg_{{ env_var('DBT_REGION') }}_db.cmn_stg_sch.stg_job_execution_log
        group by 1
    ) b on a.job_name = b.job_name 
    left join
    (select * from {{ ref('execution_logs_lookup') }}) c
    on  b.job_name = c.airflow_job_name