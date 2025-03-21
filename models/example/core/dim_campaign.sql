{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    campaign as (

        select *
        from
            (
                select
                    campaign_code, campaign_label, start_date, end_date, dbt_valid_from

                from {{ ref("stg_campaign") }}
                where dbt_valid_to is null
            ) a
        left outer join
            (
                select
                    campaign_code as min_campaign_code,
                    min(dbt_updated_at) as creation_date
                from {{ ref("stg_campaign") }}
                group by campaign_code
            ) b
            on a.campaign_code = b.min_campaign_code
    ),
    surrogate_key as (
        select p.*, hash(campaign_code) as sk_campaign_code from campaign p
    )

select
    sk_campaign_code as dcam_pk_campaign_dcam,
    campaign_code as dcam_id_campaign_dcam,
    campaign_label as dcam_lb_label_dcam,
    start_date as dcam_pk_start_date_dcam,
    end_date as dcam_lb_fg_end_date_dcam,
    creation_date as dcam_creation_date_dcam,
    dbt_valid_from as dcam_modification_date_dcam
from surrogate_key
union 
select '-1',Null,Null,Null,Null,Null,Null
