select
    assignment_profile_id_sk,
    assignment_profile_id,
    collate(assignment_profile_name, 'en-ci') as assignment_profile_name,
    assignment_profile_type,
    assignment_profile_category_code,
    collate(
        assignment_profile_category_name, 'en-ci'
    ) as assignment_profile_category_name
from {{ ref("dim_assignment_profile_referential") }}
