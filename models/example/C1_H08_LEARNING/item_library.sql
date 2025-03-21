{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    item_output as (
        select
            catalog_id as library_code,
            cpnt_id as item_code,
            cpnt_typ_id as item_type_code,
            show_in_catalog as show_in_library_flag,
            schedule_can_override_price as schedule_can_override_price_flag,
            lst_upd_usr as last_updated_by,
            lst_upd_tstmp as last_updated
        from {{ ref("stg_pa_catalog_item") }}
        where inventory_type <> 'QUALIFICATION' and dbt_valid_to is null
        qualify
            row_number() over (
                partition by catalog_id, cpnt_id, cpnt_typ_id order by rev_dte desc
            )
            = 1
    )
select
    library_code,
    item_code,
    item_type_code,
    show_in_library_flag,
    schedule_can_override_price_flag,
    last_updated_by,
    last_updated
from item_output
