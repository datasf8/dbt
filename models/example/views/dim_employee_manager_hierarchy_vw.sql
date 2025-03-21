select
    date_key,
    empl_sk_employee_id_empl as sk_employe_id,
    empl_sk_direct_manager_id_empl as sk_direct_manager_id,
    direct_manager_first_name as direct_manager_first_name_hierarchy,
    direct_manager_last_name as direct_manager_last_name_hierarchy,
    collate(
        upper(direct_manager_last_name_hierarchy)
        || ' '
        || direct_manager_first_name_hierarchy,
        'en-ci'
    ) as direct_manager_full_name_hierarchy,
    empl_sk_manager_id_empl as sk_manager_id,
    manager_first_name as manager_first_name_hierarchy,
    manager_last_name as manager_last_name_hierarchy,
    collate(
        upper(manager_last_name_hierarchy) || ' ' || manager_first_name_hierarchy,
        'en-ci'
    ) as manager_full_name_hierarchy,
    collate(
        upper(manager_last_name_hierarchy) || ' ' || manager_first_name_hierarchy,
        'en-ci'
    ) as manager_full_name_hierarchy_filter,
    level,
    -- case when level = '1' then 'Direct' else 'Indirect' end as hierarchy_level,
    -- date_key || '-' || nvl(empl_sk_employee_id_empl, '') date_employee_key
    (empl_sk_employee_id_empl || date_key)::number(38, 0) as date_employee_key
from {{ ref("dim_employee_manager_hierarchy_snapshot") }}
