{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(m.user_id, learning_job_role_id) learning_administrator_sk,
    m.user_id,
    learning_job_role_id,
    approval_role_id,
    secondary_domain_id,
    status,
    learning_administrator_code,
    learning_administrator_name,
    learning_administrator_level,
    learning_administrator_category_code,
    learning_administrator_category_name,
    learning_administrator_sub_category_code,
    learning_administrator_sub_category_name
from {{ ref("learning_administrator_structure_mapping_v1") }} m
left join
    {{ ref("learning_administrator_structure_referential_v1") }} r using (
        learning_administrator_code, learning_administrator_category_code
    )
union all
select
    -1,
    null,
    null,
    null,
    null,
    null,
    'PRACTICE_LEARNING',
    'Learning Excellence & Innovation',
    '1',
    'PRACTICE',
    'Practice',
    'PRACTICE_DIV',
    'Practice / Division'
