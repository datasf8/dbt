{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(s.schedule_id) class_sk,
    s.schedule_id as class_id,
    s.schedule_description as class_description,
    s.item_code,
    s.item_type_code,
    s.security_domain_code,
    s.is_inactive_flag,
    s.last_updated_by,
    sr.start_date as class_start_date,
    sr.end_date as class_end_date
from {{ ref("schedule_v1") }} s
left join
    {{ ref("schedule_resources_v1") }} sr
    on s.schedule_id = sr.schedule_id
    and s.schedule_data_end_date = sr.schedule_resources_data_end_date
where s.schedule_data_end_date = '9999-12-31'
union all
select -1, null, null, null, null, null, null, null, null, null
