{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    event_reasons_code,
    event_reasons_type_code,
    event_reasons_category,
    is_group_flag
from {{ ref("event_reasons_categories_seed") }}
