{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    employee_details as (
        select
            *,
            nvl(
                group_seniority,
                lag(group_seniority) ignore nulls over (
                    partition by user_id order by employment_details_start_date
                )
            ) as last_group_seniority
        from {{ ref("employment_details_v1") }}
    ),
    job_info as (
        select *
        from {{ ref("job_information_v1") }} ji
        left join
            (select distinct user_id, personal_id from employee_details) using (user_id)
        qualify
            row_number() over (
                partition by ji.user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    international_mobility as (
        select
            ji_host.user_id user_id_host,  -- ji.host.personal_id,
            ji_host.job_start_date job_start_date_host,
            me.date_month_code,
            me.date_month_end_date mobility_snapshot_date,
            ji_home.user_id user_id_home,
            ji_home.job_start_date job_start_date_home
        from job_info ji_host
        join (select 1) on employee_group_code in ('EG0008')
        join
            {{ ref("headcount_v1") }} hc
            on ji_host.user_id = hc.user_id
            and ji_host.job_start_date = hc.job_start_date
            and hc.headcount_type_code = 'STA'
            and hc.headcount_present_flag = 1
        join
            {{ ref("date_month_referential_v1") }} me
            on date_month_end_date
            between ji_host.job_start_date and ji_host.job_end_date
        left join
            job_info ji_home
            on ji_host.personal_id = ji_home.personal_id
            and ji_home.employee_group_code in ('EG0005')
            and ji_home.ec_employee_status = 'Dormant'
            and date_month_end_date
            between ji_home.job_start_date and ji_home.job_end_date
    ),
    final as (
        select
            im.*,
            host.*,
            home.cost_center_code as home_cost_center_code,
            home.job_code as home_job_code,
            home.functional_area_code as home_functional_area_code,
            home.organizational_area_code as home_organizational_area_code,
            home.brand_code as home_brand_code,
            home.employee_group_code as home_employee_group_code,
            pos.location_code as location_code,
            pos.key_position_type_code as key_position_type_code,
            posh.location_code as home_location_code,
            posh.key_position_type_code as home_key_position_type_code,
            pi.legal_gender_code,
            pi.all_player_status_id,
            gg.job_level_id,
            gg_home.job_level_id as home_job_level_id,
            nvl(
                emp.last_group_seniority, emph.last_group_seniority
            ) group_seniority_date,
            bi.date_of_birth,
            round(
                months_between(im.mobility_snapshot_date, host.job_entry_date), 3
            ) job_seniority_months,
            round(
                months_between(im.mobility_snapshot_date, group_seniority_date), 3
            ) group_seniority_months,
            round(
                months_between(im.mobility_snapshot_date, bi.date_of_birth), 3
            ) age_seniority_months,
            global_assignment_start_date,
            global_assignment_end_date,
            global_assignment_type_name_en,
            round(
                months_between(im.mobility_snapshot_date, global_assignment_start_date),
                3
            ) global_assignment_seniority_months,
            assignment_package_name_en
        from international_mobility im
        join
            job_info host
            on im.user_id_host = host.user_id
            and im.job_start_date_host = host.job_start_date
        left join {{ ref("biographical_information_v1") }} bi using (personal_id)
        left join
            job_info home
            on im.user_id_home = home.user_id
            and im.job_start_date_home = home.job_start_date
        left join
            {{ ref("position_v1") }} pos
            on host.position_code = pos.position_code
            and im.mobility_snapshot_date
            between pos.position_start_date and pos.position_end_date
        left join
            {{ ref("position_v1") }} posh
            on home.position_code = posh.position_code
            and im.mobility_snapshot_date
            between posh.position_start_date and posh.position_end_date
        left join
            employee_details emp
            on host.user_id = emp.user_id
            and im.mobility_snapshot_date
            between emp.employment_details_start_date
            and emp.employment_details_end_date
        left join
            employee_details emph
            on home.user_id = emph.user_id
            and im.mobility_snapshot_date
            between emph.employment_details_start_date
            and emph.employment_details_end_date
        left join
            {{ ref("personal_information_v1") }} pi
            on host.personal_id = pi.personal_id
            and im.mobility_snapshot_date
            between personal_info_start_date and personal_info_end_date
        left join
            {{ ref("employee_global_assignment_v1") }} ega
            on host.personal_id = ega.personal_id
            and host.user_id = ega.user_id
        left join
            {{ ref("global_assignment_type_v1") }} using (global_assignment_type_id)
        left join {{ ref("assignment_package_v1") }} using (assignment_package_id)
        left join
            {{ ref("local_pay_grade_v1") }} lpg
            on host.local_pay_grade_code = lpg.local_pay_grade_code
            and im.mobility_snapshot_date
            between local_pay_grade_start_date and local_pay_grade_end_date
        left join
            {{ ref("global_grade_v1") }} gg
            on lpg.global_grade_code = gg.global_grade_code
            and im.mobility_snapshot_date
            between global_grade_start_date and global_grade_end_date
        left join
            {{ ref("local_pay_grade_v1") }} lpg_home
            on home.local_pay_grade_code = lpg_home.local_pay_grade_code
            and im.mobility_snapshot_date
            between lpg_home.local_pay_grade_start_date
            and lpg_home.local_pay_grade_end_date
        left join
            {{ ref("global_grade_v1") }} gg_home
            on lpg_home.global_grade_code = gg_home.global_grade_code
            and im.mobility_snapshot_date
            between gg_home.global_grade_start_date and gg_home.global_grade_end_date
    )  -- select * from final
select
    to_char(f.mobility_snapshot_date, 'YYYYMM')::integer as month_sk,
    nvl(org.organization_sk, -1) as organization_sk,
    nvl(loc.location_sk, -1) as location_sk,
    nvl(kpt.key_position_type_sk, -1) as key_position_type_sk,
    nvl(ja.job_architecture_sk, -1) as job_architecture_sk,
    nvl(aj.agg_jobinfo_sk, -1) as agg_jobinfo_sk,
    nvl(org_home.organization_sk, -1) as home_organization_sk,
    nvl(loc_home.location_sk, -1) as home_location_sk,
    nvl(kpt_home.key_position_type_sk, -1) as home_key_position_type_sk,
    nvl(ja_home.job_architecture_sk, -1) as home_job_architecture_sk,
    nvl(aj_home.agg_jobinfo_sk, -1) as home_agg_jobinfo_sk,
    nvl(ae.agg_personalinfo_sk, -1) as agg_personalinfo_sk,
    nvl(jsr.range_sk, -1) as job_seniority_range_sk,
    nvl(gsr.range_sk, -1) as group_seniority_range_sk,
    nvl(asr.range_sk, -1) as age_seniority_range_sk,
    f.global_assignment_type_name_en,
    f.assignment_package_name_en,
    gasr.range_name as global_assignment_seniority_range_name,
    count(*) user_cnt
from final f
join
    {{ ref("dim_organization_v1") }} org
    on f.cost_center_code = org.cost_center_code
    and f.mobility_snapshot_date
    between organization_start_date and organization_end_date
    and org.is_ec_live_flag = true
left join
    {{ ref("dim_location_v1") }} loc
    on f.location_code = loc.location_code
    and f.mobility_snapshot_date between location_start_date and location_end_date
left join
    {{ ref("dim_key_position_type") }} kpt
    on f.key_position_type_code = kpt.key_position_type_code
    and f.mobility_snapshot_date
    between key_position_type_start_date and key_position_type_end_date
left join
    {{ ref("dim_job_architecture_v1") }} ja
    on f.job_code = ja.job_role_code
    and f.mobility_snapshot_date
    between job_architecture_start_date and job_architecture_end_date
left join
    {{ ref("dim_agg_jobinfo") }} aj
    on nvl(f.functional_area_code, 'NULL') = nvl(aj.functional_area_code, 'NULL')
    and nvl(f.organizational_area_code, 'NULL')
    = nvl(aj.organizational_area_code, 'NULL')
    and nvl(f.brand_code, 'NULL') = nvl(aj.brand_code, 'NULL')
    and nvl(f.employee_group_code, 'NULL') = nvl(aj.employee_group_code, 'NULL')
    and nvl(f.job_level_id, 'NULL') = nvl(aj.job_level_id, 'NULL')
left join
    {{ ref("dim_organization_v1") }} org_home
    on f.home_cost_center_code = org_home.cost_center_code
    and f.mobility_snapshot_date
    between org_home.organization_start_date and org_home.organization_end_date
left join
    {{ ref("dim_location_v1") }} loc_home
    on f.home_location_code = loc_home.location_code
    and f.mobility_snapshot_date
    between loc_home.location_start_date and loc_home.location_end_date
left join
    {{ ref("dim_key_position_type") }} kpt_home
    on f.home_key_position_type_code = kpt_home.key_position_type_code
    and f.mobility_snapshot_date
    between kpt_home.key_position_type_start_date
    and kpt_home.key_position_type_end_date
left join
    {{ ref("dim_job_architecture_v1") }} ja_home
    on f.home_job_code = ja_home.job_role_code
    and f.mobility_snapshot_date
    between ja_home.job_architecture_start_date and ja_home.job_architecture_end_date
left join
    {{ ref("dim_agg_jobinfo") }} aj_home
    on nvl(f.home_functional_area_code, 'NULL')
    = nvl(aj_home.functional_area_code, 'NULL')
    and nvl(f.home_organizational_area_code, 'NULL')
    = nvl(aj_home.organizational_area_code, 'NULL')
    and nvl(f.home_brand_code, 'NULL') = nvl(aj_home.brand_code, 'NULL')
    and nvl(f.home_employee_group_code, 'NULL')
    = nvl(aj_home.employee_group_code, 'NULL')
    and nvl(f.home_job_level_id, 'NULL') = nvl(aj_home.job_level_id, 'NULL')
left join
    {{ ref("dim_agg_personalinfo") }} ae
    on nvl(f.legal_gender_code, 'NULL') = nvl(ae.legal_gender_code, 'NULL')
    and nvl(f.all_player_status_id, 'NULL') = nvl(ae.all_players_status_id, 'NULL')
left join
    {{ ref("dim_range_v1") }} jsr
    on jsr.range_type = 'JOBSEN'
    and job_seniority_months >= range_start
    and job_seniority_months < range_end
left join
    {{ ref("dim_range_v1") }} gsr
    on gsr.range_type = 'GRPSEN'
    and group_seniority_months >= gsr.range_start
    and group_seniority_months < gsr.range_end
left join
    {{ ref("dim_range_v1") }} asr
    on asr.range_type = 'AGESEN'
    and age_seniority_months / 12 >= asr.range_start
    and age_seniority_months / 12 < asr.range_end
left join
    {{ ref("dim_range_v1") }} gasr
    on gasr.range_type = 'GASSEN'
    and global_assignment_seniority_months / 12 >= gasr.range_start
    and global_assignment_seniority_months / 12 < gasr.range_end
group by all
