{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    qual as (
        select
            userid,
            startdate,
            enddate,
            timetype,
            approvalstatus,
            cust_ampmforfractionleave,
            quantityinhours,
            quantityindays
        from {{ ref("stg_employeetime_flatten") }}
        where dbt_valid_to is null
        qualify
            row_number() over (
                partition by
                    userid,
                    startdate,
                    enddate,
                    timetype,
                    approvalstatus,
                    cust_ampmforfractionleave
                order by startdate

            )
            = 1
    )
select
    userid as user_id,
    startdate as time_start_date,
    enddate as time_end_date,
    timetype as time_type_code,
    sum(quantityinhours) as quantity_in_hours,
    sum(quantityindays) as quantity_in_days,
    approvalstatus as approval_status
from qual
group by userid, startdate, enddate, timetype, approvalstatus
