{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    time_type::varchar(1000) collate 'en-ci' as time_type,
    category::varchar(1000) collate 'en-ci' as category
from {{ ref("leave_category_seed") }}
