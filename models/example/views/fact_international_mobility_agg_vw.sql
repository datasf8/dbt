{{ config(schema="cmn_pub_sch") }}
select
    month_sk,
    collate('Host', 'en-ci') as type,
    organization_sk,
    location_sk,
    key_position_type_sk,
    job_architecture_sk,
    agg_jobinfo_sk as agg_jobinfo_sk,
    home_organization_sk as organization_1_sk,
    home_job_architecture_sk as job_architecture_1_sk,
    agg_personalinfo_sk,
    job_seniority_range_sk,
    group_seniority_range_sk,
    age_seniority_range_sk,
    global_assignment_type_name_en,
    assignment_package_name_en,
    global_assignment_seniority_range_name,
    user_cnt
from {{ ref("fact_international_mobility_agg") }}
union all
select
    month_sk,
    collate('Home', 'en-ci') as type,
    home_organization_sk as organization_sk,
    home_location_sk as location_sk,
    home_key_position_type_sk as key_position_type_sk,
    home_job_architecture_sk as job_architecture_sk,
    home_agg_jobinfo_sk as agg_jobinfo_sk,
    organization_sk as organization_1_sk,
    job_architecture_sk as job_architecture_1_sk,
    agg_personalinfo_sk,
    job_seniority_range_sk,
    group_seniority_range_sk,
    age_seniority_range_sk,
    global_assignment_type_name_en,
    assignment_package_name_en,
    global_assignment_seniority_range_name,
    user_cnt
from {{ ref("fact_international_mobility_agg") }}
