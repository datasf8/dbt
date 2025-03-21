{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    item_type_id,
    spcs.cpnt_typ_id as item_type_code,
    spcs.dbt_valid_from as item_type_start_date,
    nvl(spcs.dbt_valid_to, '9999-12-31') as item_type_end_date,
    spall.label_value as item_type_description,
    spcs.entity_type as entity_type
from
    (
        select
            hash(cpnt_typ_id,dbt_valid_from) as item_type_id,
            cpnt_typ_id,
            dbt_valid_from,
            dbt_valid_to,
            cpnt_typ_desc,
            entity_type
        from {{ ref("stg_pa_cpnt_type") }}
        where dbt_valid_to is null
    ) spcs
left outer join
    (
        select label_id, label_value
        from {{ ref("stg_pv_i18n_active_locale_label") }}
        where locale_id = 'English' and dbt_valid_to is null
    ) spall
    on cpnt_typ_desc = label_id
