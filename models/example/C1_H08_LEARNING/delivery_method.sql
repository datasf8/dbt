{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    hash(del_mth_id, dbt_valid_from) as delivery_method_id,
    del_mth_id as delivery_method_code,
    dbt_valid_from as delivery_method_start_date,
    nvl(dbt_valid_to,'9999-12-31') as delivery_method_end_date,
    del_mth_desc as delivery_method_description
from {{ ref("stg_pa_del_method") }}
where dbt_valid_to is null
