{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
with
    base as (
        with
            wsf as

            (
                select
                    work_schedule_code as externalcode,
                    work_schedule_start_date as startingdate,
                    average_hours_per_day as averagehoursperday
                from {{ ref("work_schedule_v1") }}
            -- and externalcode = 'CHL_MTWTFSS40Hrs_RETAIL'
            ),
            wsdf as (
                select
                    work_schedule_code as workschedule_externalcode,
                    to_number(day_number) as ws_day,
                    planed_hours as workinghours
                from {{ ref("work_schedule_day_v1") }}
                union

                select
                    work_schedule_code as workschedule_externalcode,
                    to_number(day_number) as ws_day,
                    left(nvl(work_schedule_day_model_code, '0'), 1) as workinghours
                from {{ ref("work_schedule_day_model_assignment_v1") }}

            -- and workschedule_externalcode = 'CHL_MTWTFSS40Hrs_RETAIL'
            ),
            emp_time as (
                select
                    user_id as et_userid,
                    time_start_date as et_startdate,
                    time_end_date as et_enddate,
                    time_type_code as timetype,
                    quantity_in_hours as quantityinhours,
                    quantity_in_days as quantityindays,
                    approval_status as approvalstatus
                from {{ ref("employee_time_v1") }}
                where approval_status = 'APPROVED'
            ),
            wsdf_max as (
                select
                    work_schedule_code as workschedule_externalcode,
                    max(to_number(day_number)) as maxday
                from {{ ref("work_schedule_day_v1") }}
                group by workschedule_externalcode
                union
                select
                    work_schedule_code as workschedule_externalcode,
                    max(to_number(day_number)) as maxday
                from {{ ref("work_schedule_day_model_assignment_v1") }}
                group by workschedule_externalcode
            ),
            emp_job1 as (
                select
                    user_id as ej_userid,
                    job_start_date as ej_startdate,
                    job_end_date as ej_enddate,
                    work_schedule_code as workschedulecode,
                    holiday_calendar_code as holidaycalendarcode,
                    position_code

                from {{ ref("job_information") }}
                -- where user_id = '00802661'
                qualify
                    row_number() over (
                        partition by user_id, job_start_date
                        order by sequence_number desc
                    )
                    = 1
            ),
            emp_job as (
                select
                    ej_userid,
                    ej_startdate,
                    ej_enddate,
                    workschedulecode,
                    holidaycalendarcode,
                    position_code,
                    lag(position_code) ignore nulls over (
                        partition by ej_userid order by ej_startdate
                    ) as last_position_code
                from emp_job1
            ),
            hol as (
                select
                    date_of_holiday as hol_date,
                    holiday_calendar_code as holidaycalendar_externalcode,
                    holiday_name as holiday,
                    holiday_category_code as holidaycategory
                from {{ ref("holiday_assignment_v1") }}
            )
        select
            et_userid as user_id,
            nvl(position_code, last_position_code) as position_code,
            timetype as time_type_code,
            workschedulecode as work_schedule_code,
            holidaycalendarcode as time_holiday_code,
            startingdate as starting_date,
            et_startdate as start_date,
            et_enddate as end_date,
            range_date as time_date,
            hol_date,
            workinghours as working_hours,
            quantityindays as time_quantity_in_day,
            quantityinhours as quantity_in_hours,
            mod((range_date - startingdate), maxday) + 1 as time_weekday_number_1,
            dayname(range_date) as time_weekday_name,
            extract('dayofweek', range_date) as time_weekday_number,
            case
                when workinghours = 0
                then 0
                when hol_date is not null
                then 0
                when et_startdate = et_enddate
                then quantityindays
                else 1
            end as calc_days
        from
            (
                select
                    et_userid,
                    timetype,
                    et_startdate,
                    et_enddate,
                    quantityinhours,
                    quantityindays,

                    date(et_startdate) + value::int as range_date,
                    dayname(range_date)
                from
                    emp_time,
                    table(
                        flatten(
                            array_generate_range(
                                0, datediff('day', et_startdate, et_enddate) + 1
                            )
                        )
                    )
                order by range_date desc
            ) et_range
        join
            emp_job ej
            on et_range.et_userid = ej_userid
            and et_range.range_date between ej.ej_startdate and ej.ej_enddate
        join wsdf_max wm on ej.workschedulecode = wm.workschedule_externalcode
        join wsf ws on ej.workschedulecode = ws.externalcode
        left join
            wsdf
            on wm.workschedule_externalcode = wsdf.workschedule_externalcode
            and mod((range_date::date - startingdate::date), maxday) + 1 = ws_day
        left join
            hol
            on ej.holidaycalendarcode = hol.holidaycalendar_externalcode
            and range_date = hol_date
    )
select
    user_id,
    time_type_code,
    time_date,
    time_weekday_number,
    time_weekday_name,
    calc_days as time_quantity_in_day
from (select * from base where calc_days <> 0) base
