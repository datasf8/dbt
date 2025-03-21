{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
select
    hash(cmpl_stat_id, spcs.dbt_valid_from) as completionstatus_id,
    cmpl_stat_id as completion_status_code,
    spcs.dbt_valid_from as completion_status_start_date,
    nvl(spcs.dbt_valid_to, '9999-12-31') as completion_status_end_date,
    label_value as completion_status_description,
    cpnt_typ_id as item_type_code,
    provide_crdt as provides_credit
from
    (
        select
            cmpl_stat_id,
            dbt_valid_from,
            dbt_valid_to,
            cmpl_stat_desc,
            cpnt_typ_id,
            provide_crdt
        from {{ ref("stg_pa_cmpl_stat") }}
    ) spcs
left outer join
    (
        select *
        from {{ ref("stg_pv_i18n_active_locale_label") }}
        where dbt_valid_to is null and locale_id = 'English'
    ) spall
    on cmpl_stat_desc = label_id
