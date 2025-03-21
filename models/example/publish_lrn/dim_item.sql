{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(item_code) as item_sk,
    item_code,
    item_creation_date,
    item_title,
    item_description,
    item_type_code,
    item_is_inactive_flag as is_inactive_flag,
    provider_code,
    delivery_method_code,
    contact
from {{ ref("item_v1") }}
where item_data_end_date = '9999-12-31'
union all
select -1, null, null, null, null, null, null, null, null, null
