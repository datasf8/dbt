{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    check_ttp as (
        select
            time_type_profile_code,
            max(
                iff(
                    not contains(et.time_type_code, 'REMOTE')
                    and not contains(et.time_type_code, 'HOME')
                    and not contains(et.time_type_code, 'FLEX_WORK')
                    and not contains(et.time_type_code, 'SMART'),
                    'Yes',
                    'No'
                )
            ) is_using_timeoff_flag,
            max(
                iff(
                    (
                        contains(et.time_type_code, 'REMOTE')
                        or contains(et.time_type_code, 'HOME')
                        or contains(et.time_type_code, 'FLEX_WORK')
                        or contains(et.time_type_code, 'SMART')
                    )
                    and nvl(bu.business_unit_type_code, '') not in ('3', '03', '15')
                    and nvl(sp.professional_field_code, '') not in ('PF000024'),
                    'Yes',
                    'No'
                )
            ) is_using_remote_flag
        from {{ ref("employee_time_v1") }} et
        join
            {{ ref("time_type_v1") }} tt
            on et.time_type_code = tt.time_type_code
            and loa_start_event_reason is null
        join
            {{ ref("job_information_v1") }} ji
            on ji.user_id = et.user_id
            and et.time_start_date between ji.job_start_date and ji.job_end_date
        left join
            {{ ref("cost_center_v1") }} cc
            on ji.cost_center_code = cc.cost_center_code
            and et.time_start_date
            between cc.cost_center_start_date and cc.cost_center_end_date
        left join
            {{ ref("business_unit_v1") }} bu
            on cc.business_unit_code = bu.business_unit_code
            and et.time_start_date
            between business_unit_start_date and business_unit_end_date
        left join
            {{ ref("job_role_v1") }} jr
            on ji.job_code = jr.job_role_code
            and et.time_start_date
            between jr.job_role_start_date and jr.job_role_end_date
        left join
            {{ ref("specialization_v1") }} sp
            on jr.specialization_code = sp.specialization_code
            and et.time_start_date
            between sp.specialization_start_date and sp.specialization_end_date
        group by all
    )
select
    pttp.* exclude (time_type_profile_status),
    nvl(is_using_timeoff_flag, 'No') is_using_timeoff_flag,
    nvl(is_using_remote_flag, 'No') is_using_remote_flag,
    time_type_profile_status
from {{ ref("pre_time_type_profile") }} pttp
left join check_ttp cttp on pttp.time_type_profile_code = cttp.time_type_profile_code
