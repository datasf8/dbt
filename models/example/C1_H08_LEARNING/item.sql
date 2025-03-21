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
    -- hash(cpnt_id, dbt_valid_from) as item_id,
    hash(cpnt_id, dbt_valid_from, cpnt_typ_id, rev_dte) as item_id,
    cpnt_id as item_code,
    dbt_valid_from as item_data_start_date,
    iff(
        dbt_valid_to is null,
        lead(dbt_valid_from, 1, {d '9999-12-31'}) over (
            partition by cpnt_id order by dbt_valid_from
        ),
        dbt_valid_to
    ) as item_data_end_date,
    create_dte as item_creation_date,
    b.label_value as item_title,
    c.label_value as item_description,
    cpnt_typ_id as item_type_code,
    dmn_id as security_domain_code,
    rev_dte as item_revision_date,
    cpnt_src_id as provider_code,
    del_mth_id as delivery_method_code,
    notactive as item_is_inactive_flag,
    contact,
    cpnt_classification as item_classification,
    cpnt_len as item_length,
    d.label_value as target_audience,
    lst_upd_tstmp as last_udpdated,
    lst_upd_usr as last_updated_by,
    e.item_type_id as item_type_id,
    f.security_domain_id as domain_id,
    g.provider_id,
    h.delivery_method_id

from
    (
        select
            cpnt_id,
            dbt_valid_from,
            dbt_valid_to,
            create_dte,
            cpnt_title,
            cpnt_desc,
            cpnt_typ_id,
            rev_dte,
            dmn_id,
            cpnt_src_id,
            del_mth_id,
            notactive,
            contact,
            cpnt_classification,
            cpnt_len,
            tgt_audnc,
            lst_upd_usr,
            lst_upd_tstmp
        from {{ ref("stg_pa_cpnt") }} 
    -- qualify
    -- row_number() over (
    -- partition by cpnt_id, dbt_valid_from order by rev_dte desc
    -- )
    -- = 1
    ) a
left outer join
    (
        select label_id, label_value
        from {{ ref("stg_pv_i18n_active_locale_label") }}
        where locale_id = 'English' and dbt_valid_to is null
    ) b
    on a.cpnt_title = b.label_id
left outer join
    (
        select label_id, label_value
        from {{ ref("stg_pv_i18n_active_locale_label") }}
        where locale_id = 'English' and dbt_valid_to is null
    ) c
    on a.cpnt_desc = c.label_id

left outer join
    (
        select label_id, label_value
        from {{ ref("stg_pv_i18n_active_locale_label") }}
        where locale_id = 'English' and dbt_valid_to is null
    ) d
    on a.tgt_audnc = d.label_id
left outer join
    (
        select *
        from {{ ref("item_type_v1") }}
        qualify
            row_number() over (
                partition by item_type_code order by item_type_start_date desc
            )
            = 1
    ) e
    on a.cpnt_typ_id = e.item_type_code
left outer join
    (
        select *
        from {{ ref("security_domain_v1") }}
        qualify
            row_number() over (
                partition by security_domain_code
                order by security_domain_start_date desc
            )
            = 1
    ) f
    on a.dmn_id = f.security_domain_code
left outer join
    (
        select *
        from {{ ref("provider_v1") }}
        qualify
            row_number() over (
                partition by provider_code order by provider_start_date desc
            )
            = 1
    ) g
    on a.cpnt_src_id = g.provider_code
left outer join
    (
        select *
        from {{ ref("delivery_method_v1") }}
        qualify
            row_number() over (
                partition by delivery_method_code
                order by delivery_method_start_date desc
            )
            = 1
    ) h
    on a.del_mth_id = h.delivery_method_code
    