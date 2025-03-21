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
    termination_event_reasons_type_code,
    termination_event_reasons_category
from {{ ref("termination_event_reasons_seed") }}
