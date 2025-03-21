{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select event_reasons_code
from {{ ref("hiring_event_reasons_seed") }}
