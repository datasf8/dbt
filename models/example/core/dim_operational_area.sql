{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    opera_area as (
        select distinct *
        from {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_ec_operational_area
        where opar_sk_opar <> '-1'
    )
select
    opar_sk_opar as opar_pk_opar,
    opar_code_opar,
    opar_label_opar,
    opar_externalname_en_us_opar,
    opar_externalname_ro_ro_opar,
    opar_externalname_pt_br_opar,
    opar_externalname_th_th_opar,
    opar_externalname_cs_cz_opar,
    opar_externalname_hu_hu_opar,
    opar_externalname_nl_nl_opar,
    opar_externalname_da_dk_opar,
    opar_externalname_ko_kr_opar,
    opar_externalname_it_it_opar,
    opar_externalname_ru_ru_opar,
    opar_externalname_el_gr_opar,
    opar_externalname_pl_pl_opar,
    opar_externalname_tr_tr_opar,
    opar_externalname_fr_fr_opar,
    opar_externalname_ja_jp_opar,
    opar_externalname_de_de_opar,
    opar_externalname_zh_tw_opar,
    opar_externalname_bs_id_opar,
    opar_externalname_fr_ca_opar,
    opar_externalname_bs_bs_opar,
    opar_externalname_vi_vn_opar,
    opar_externalname_fi_fi_opar,
    opar_externalname_en_gb_opar,
    opar_externalname_zh_cn_opar,
    opar_externalname_pt_pt_opar,
    opar_externalname_ar_sa_opar,
    opar_externalname_es_mx_opar,
    opar_externalname_es_es_opar,
    opar_externalname_iw_il_opar,
    opar_externalname_nb_no_opar,
    opar_externalname_sv_se_opar,
    opar_externalname_uk_ua_opar,
    opar_externalname_en_debug_opar,
    opar_effectivestartdate_opar,
    opar_mdfsystemstatus_opar
from opera_area
union 
select '-1',NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,
NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null,NUll,NUll,NUll,Null,Null