{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
        post_hook="USE DATABASE {{ env_var('DBT_CORE_DB') }};use schema CMN_CORE_SCH;
        call rls_policy_apply_sp('{{ database }}','{{ schema }}','CSRD_HEADCOUNT_DETAILS');"
    )
}}


with
    one_profile_go_live as (

        select company_v1.company_id
        from {{ ref("one_profile_go_live_dates_v1") }} go_live_dates
        join
            {{ ref("company_v1") }} company_v1
            on go_live_dates.company_code = company_v1.company_code
        where ec_go_live_date is not null

    )




select
    hash(base.user_id, base.dt_begin) as csrd_hd_id,
    base.dt_begin as csrd_hd_start_date,
    base.dt_end as csrd_hd_end_date,
    base.personal_id,
    base.user_id,
    personal_information.legal_first_name,
    personal_information.legal_last_name,
    biographical_information.date_of_birth,
    legal_gender.legal_gender_code,
    legal_gender.legal_gender_name_en,
    employee_status.employee_status_code,
    employee_status_name_en,
    local_contract_type.local_contract_type_code,
    local_contract_type.local_contract_type_name_en,
    employee_group.employee_group_code,
    employee_group.employee_group_name_en,
    employee_subgroup.employee_subgroup_code,
    employee_subgroup.employee_subgroup_name_en,
    cost_center.cost_center_code,
    cost_center.cost_center_name_en,
    bu.business_unit_code,
    bu.business_unit_name_en,
    bu_type.business_unit_type_code,
    bu_type.business_unit_type_name_en,
    company.company_code,
    company.company_name_en,
    country.country_code,
    country.country_name_en,
    geographic_zone.geographic_zone_code,
    geographic_zone.geographic_zone_name_en,
    disability_status.disability_status_code,
    disability_status.disability_status_name_en,
    key_position_type.key_position_type_code,
    key_position_type.key_position_type_name_en,
    base.termination_date, 
    0 as flag_purged
from {{ ref("_tmp_csrd_employee_sub_group") }} base
join one_profile_go_live go_live on base.company_id = go_live.company_id
left outer join
    {{ ref("legal_gender_v1") }} legal_gender
    on base.legal_gender_id = legal_gender.legal_gender_id
left outer join
    {{ ref("employee_status_v1") }} employee_status
    on base.employee_status_id = employee_status.employee_status_id
left outer join
    {{ ref("local_contract_type_v1") }} local_contract_type
    on base.local_contract_type_id = local_contract_type.local_contract_type_id
left outer join
    {{ ref("employee_group_v1") }} employee_group
    on base.employee_group_id = employee_group.employee_group_id
left outer join
    {{ ref("cost_center_v1") }} cost_center
    on base.cost_center_id = cost_center.cost_center_id
left outer join
    {{ ref("business_unit_v1") }} bu on base.business_unit_id = bu.business_unit_id

left outer join
    {{ ref("business_unit_type_v1") }} bu_type
    on base.business_unit_type_id = bu_type.business_unit_type_id

left outer join {{ ref("company_v1") }} company on base.company_id = company.company_id
left outer join {{ ref("country_v1") }} country on base.country_id = country.country_id
left outer join
    {{ ref("geographic_zone_v1") }} geographic_zone
    on base.geographic_zone_id = geographic_zone.geographic_zone_id
left outer join
    {{ ref("disability_status_v1") }} disability_status
    on base.disability_status_id = disability_status.disability_status_id
left outer join
    {{ ref("key_position_type_v1") }} key_position_type
    on base.key_position_type_id = key_position_type.key_position_type_id
left outer join
    {{ ref("employee_subgroup_v1") }} employee_subgroup
    on base.employee_subgroup_id = employee_subgroup.employee_subgroup_id
left outer join
    {{ ref("personal_information_v1") }} personal_information
    on base.personal_id = personal_information.personal_id
    and base.dt_begin <= personal_information.personal_info_end_date
    and base.dt_end >= personal_information.personal_info_start_date
left outer join
    {{ ref("biographical_information_v1") }} biographical_information
    on base.personal_id = biographical_information.personal_id
