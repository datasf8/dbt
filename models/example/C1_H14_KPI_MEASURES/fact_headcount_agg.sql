{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        snowflake_warehouse="HRDP_DBT_PREM_WH",
    )
}}
with
    job_information as (
        select *
        from {{ ref("job_information") }}
        qualify
            row_number() over (
                partition by user_id, job_start_date order by sequence_number desc
            )
            = 1
    ),
    data1 as (
        select
            dm.date_month_code,
            hc.headcount_type_code,
            lg.legal_gender_id,
            cn.country_id as country_of_nationality_id,
            es.employee_status_id,
            eg.employee_group_id,
            esg.employee_subgroup_id,
            jr.job_role_id,
            sp.specialization_id,
            pf.professional_field_id,
            fa.functional_area_id,
            oa.organizational_area_id,
            cs.cost_center_id,
            bu.business_unit_id,
            but.business_unit_type_id as type_of_business_unit_id,
            ar.area_id,
            hd.hr_division_id,
            co.company_id,
            cntry.country_id as country_id,
            br.brand_id,
            loc.location_id,
            kpt.key_position_type_id,
            ji.local_contract_type_id,
            gg.global_grade_id,
            lpg.local_pay_grade_id,
            gg.job_level_id,
            round(
                months_between(dm.date_month_end_date, ed.group_seniority), 3
            ) as group_seniority_in_months,
            round(
                months_between(dm.date_month_end_date, ji.job_entry_date), 3
            ) as job_seniority_in_months,
            round(
                months_between(dm.date_month_end_date, bi.date_of_birth) / 12, 3
            ) as age_seniority_in_years,
            gsr.range_id as group_seniority_range_id,
            jsr.range_id as job_seniority_range_id,
            asr.range_id as age_seniority_range_id,
            pi.all_player_status_id,
            piu.ethnicity_id,
            piu.race_1_id as race_id,
            count(*) as headcount_number,
            count_if(all_player_status_id is not null) as key_players_number,
            count_if(
                pos.key_position_type_code in ('SKP', 'GKP', 'LKP')
            ) as new_key_position_assignment_number,
            sum(group_seniority_in_months) as total_group_seniority_in_months,
            sum(job_seniority_in_months) as total_job_seniority_in_months,
            sum(age_seniority_in_years) as total_age_seniority_in_years
        from {{ ref("date_month_referential") }} dm
        join
            {{ ref("headcount") }} hc
            on dm.date_month_end_date between hc.job_start_date and hc.job_end_date
            and hc.headcount_present_flag = 1
        join job_information ji using (user_id, job_start_date)
        left join
            {{ ref("employment_details") }} ed
            on ji.user_id = ed.user_id
            and dm.date_month_end_date
            between ed.employment_details_start_date and ed.employment_details_end_date
        left join
            {{ ref("personal_information") }} pi
            on ed.personal_id = pi.personal_id
            and dm.date_month_end_date
            between pi.personal_info_start_date and pi.personal_info_end_date
        left join
            {{ ref("legal_gender") }} lg
            on pi.legal_gender_code = lg.legal_gender_code
            and dm.date_month_end_date
            between lg.legal_gender_start_date and lg.legal_gender_end_date
        left join
            {{ ref("country") }} cn
            on pi.country_code = cn.country_code
            and dm.date_month_end_date
            between cn.country_start_date and cn.country_end_date
        left join
            {{ ref("employee_status") }} es
            on ji.employee_status_id = es.employee_status_id
            and dm.date_month_end_date
            between es.employee_status_start_date and es.employee_status_end_date
        left join
            {{ ref("employee_group") }} eg
            on ji.employee_group_code = eg.employee_group_code
            and dm.date_month_end_date
            between eg.employee_group_start_date and eg.employee_group_end_date
        left join
            {{ ref("employee_subgroup") }} esg
            on ji.employee_subgroup_code = esg.employee_subgroup_code
            and dm.date_month_end_date
            between esg.employee_subgroup_start_date and esg.employee_subgroup_end_date
        left join
            {{ ref("job_role") }} jr
            on ji.job_role_code = jr.job_role_code
            and dm.date_month_end_date between job_role_start_date and job_role_end_date
        left join
            {{ ref("specialization") }} sp
            on jr.specialization_code = sp.specialization_code
            and dm.date_month_end_date
            between specialization_start_date and specialization_end_date
        left join
            {{ ref("professional_field") }} pf
            on sp.professional_field_code = pf.professional_field_code
            and dm.date_month_end_date
            between professional_field_start_date and professional_field_end_date
        left join
            {{ ref("functional_area") }} fa
            on ji.functional_area_code = fa.functional_area_code
            and dm.date_month_end_date
            between functional_area_start_date and functional_area_end_date
        left join
            {{ ref("organizational_area") }} oa
            on ji.organizational_area_code = oa.organizational_area_code
            and dm.date_month_end_date
            between organizational_area_start_date and organizational_area_end_date
        left join
            {{ ref("cost_center") }} cs
            on ji.cost_center_code = cs.cost_center_code
            and dm.date_month_end_date
            between cost_center_start_date and cost_center_end_date
        left join
            {{ ref("business_unit") }} bu
            on cs.business_unit_code = bu.business_unit_code
            and dm.date_month_end_date
            between business_unit_start_date and business_unit_end_date
        left join
            {{ ref("business_unit_type") }} but
            on bu.business_unit_type_code = but.business_unit_type_code
            and dm.date_month_end_date
            between business_unit_type_start_date and business_unit_type_end_date
        left join
            {{ ref("area") }} ar
            on bu.area_code = ar.area_code
            and dm.date_month_end_date between area_start_date and area_end_date
        left join
            {{ ref("hr_division") }} hd
            on ar.hr_division_code = hd.hr_division_code
            and dm.date_month_end_date
            between hr_division_start_date and hr_division_end_date
        left join
            {{ ref("company") }} co
            on bu.company_code = co.company_code
            and dm.date_month_end_date between company_start_date and company_end_date
        left join
            {{ ref("country") }} cntry
            on co.country_code = cntry.country_code
            and dm.date_month_end_date
            between cntry.country_start_date and cntry.country_end_date
        left join
            {{ ref("brand") }} br
            on ji.brand_code = br.brand_code
            and dm.date_month_end_date between brand_start_date and brand_end_date
        left join
            {{ ref("position") }} pos
            on ji.position_code = pos.position_code
            and dm.date_month_end_date between position_start_date and position_end_date
        left join
            {{ ref("location") }} loc
            on pos.location_code = loc.location_code
            and dm.date_month_end_date between location_start_date and location_end_date
        left join
            {{ ref("key_position_type") }} kpt
            on pos.key_position_type_code = kpt.key_position_type_code
            and dm.date_month_end_date
            between key_position_type_start_date and key_position_type_end_date
        left join
            {{ ref("local_pay_grade") }} lpg
            on ji.local_pay_grade_code = lpg.local_pay_grade_code
            and dm.date_month_end_date
            between local_pay_grade_start_date and local_pay_grade_end_date
        left join
            {{ ref("global_grade") }} gg
            on lpg.global_grade_code = gg.global_grade_code
            and dm.date_month_end_date
            between global_grade_start_date and global_grade_end_date
        left join
            {{ ref("biographical_information") }} bi on ed.personal_id = bi.personal_id
        left join
            {{ ref("personal_information_local_usa") }} piu
            on ed.personal_id = piu.personal_id
            and dm.date_month_end_date
            between personal_info_usa_start_date and personal_info_usa_end_date
        left join
            {{ ref("ranges_v1") }} gsr
            on gsr.range_type = 'GRPSEN'
            and group_seniority_in_months
            between gsr.range_start and (gsr.range_end - 0.001)
        left join
            {{ ref("ranges_v1") }} jsr
            on jsr.range_type = 'JOBSEN'
            and job_seniority_in_months
            between jsr.range_start and (jsr.range_end - 0.001)
        left join
            {{ ref("ranges_v1") }} asr
            on asr.range_type = 'AGESEN'
            and age_seniority_in_years
            between asr.range_start and (asr.range_end - 0.001)
        group by all
    )
select
    * exclude (
        group_seniority_in_months, job_seniority_in_months, age_seniority_in_years
    )
from data1
