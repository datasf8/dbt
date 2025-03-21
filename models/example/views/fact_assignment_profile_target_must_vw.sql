{{ config(schema="lrn_pub_sch") }}

select
    personal_id,
    user_id as assigment_profile_user_id,
    assignment_profile_id_sk,
    organization_bycompany_sk,
    employee_profile_sk,
    assignment_profile_type,
    completion_status,
    completion_flag,
    ec_ep_status,
    ec_status,
    manager_email,
    collate(plant, 'en-ci') as plant,
    collate(dc, 'en-ci') as dc,
    collate(ba, 'en-ci') as ba,
    collate(intern, 'en-ci') as intern,
    collate(apprentice, 'en-ci') as apprentice,
    completion_date,
    completed
from {{ ref("fact_assignment_profile_target") }}
where assignment_profile_type = 'MUST'
