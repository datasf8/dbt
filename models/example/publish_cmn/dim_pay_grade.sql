{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    local_pay_grade_id as pay_grade_sk,
    local_pay_grade_code,
    local_pay_grade_start_date,
    local_pay_grade_end_date,
    local_pay_grade_name_en,
    local_pay_grade_name_en
    || ' ('
    || local_pay_grade_code
    || ')' as local_pay_grade_name_label,
    local_pay_grade_status,
    country_id,
    country_code,
    nvl(lg.global_grade_id, -1) as global_grade_id,
    global_grade_code,
    global_grade_name,
    global_grade_name || ' (' || global_grade_code || ')' as global_grade_name_label,
    nvl(gg.job_level_id::int, -1) as job_level_id,
    job_level_code,
    job_level_name_en,
    job_level_name_en || ' (' || job_level_code || ')' as job_level_name_label
from {{ ref("local_pay_grade") }} lg
left join {{ ref("global_grade") }} gg using (global_grade_id)
left join {{ ref("job_level") }} jl using (job_level_id)
union all
select
    -1,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
