select distinct grp_id, employee_id from {{ ref("dim_group_user") }}
