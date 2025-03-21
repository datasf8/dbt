{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, startdate) as global_grade_id,
    externalcode as global_grade_code,
    startdate as global_grade_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as global_grade_end_date,
    name as global_grade_name,
    customstring1 as job_level_id,
    status as global_grade_status
from {{ ref("stg_fo_pay_grade_flatten") }}
where dbt_valid_to is null
