{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}


with
    scope_typ as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_scope_type
        where scot_sk_scot <> '-1'
    )
select
    scot_sk_scot as scot_pk_scot,
    scot_code_scot,
    scot_label_scot,
    scot_externalname_en_us_scot,
    scot_externalname_ro_ro_scot,
    scot_externalname_pt_br_scot,
    scot_externalname_th_th_scot,
    scot_externalname_cs_cz_scot,
    scot_externalname_hu_hu_scot,
    scot_externalname_nl_nl_scot,
    scot_externalname_da_dk_scot,
    scot_externalname_ko_kr_scot,
    scot_externalname_it_it_scot,
    scot_externalname_ru_ru_scot,
    scot_externalname_el_gr_scot,
    scot_externalname_pl_pl_scot,
    scot_externalname_tr_tr_scot,
    scot_externalname_fr_fr_scot,
    scot_externalname_ja_jp_scot,
    scot_externalname_de_de_scot,
    scot_externalname_zh_tw_scot,
    scot_externalname_bs_id_scot,
    scot_externalname_fr_ca_scot,
    scot_externalname_bs_bs_scot,
    scot_externalname_vi_vn_scot,
    scot_externalname_fi_fi_scot,
    scot_externalname_en_gb_scot,
    scot_externalname_zh_cn_scot,
    scot_externalname_pt_pt_scot,
    scot_externalname_ar_sa_scot,
    scot_externalname_es_mx_scot,
    scot_externalname_es_es_scot,
    scot_externalname_iw_il_scot,
    scot_externalname_nb_no_scot,
    scot_externalname_sv_se_scot,
    scot_externalname_uk_ua_scot,
    scot_externalname_en_debug_scot,
    scot_mdfsystemstatus_scot,
    scot_effectivestartdate_scot
from scope_typ
union
select '-1',NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null,
NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null