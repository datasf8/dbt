{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    learning_administrator_id as learning_administrator_code,
    learning_administrator_label as learning_administrator_name,
    administrator_level as learning_administrator_level,
    administrator_category_id as learning_administrator_category_code,
    administrator_category_label as learning_administrator_category_name,
    administrator_sub_category_id as learning_administrator_sub_category_code,
    administrator_sub_category as learning_administrator_sub_category_name
from {{ ref("stg_lms_admin_structure_referential") }}
where dbt_valid_to is null
