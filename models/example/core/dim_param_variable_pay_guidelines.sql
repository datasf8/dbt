{{
    config(
        materialized="table",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
with
    guidelines as (
        select
            rating,
            min_guidelines,
            max_guidelines,
            dpvp_pk_variable_pay_campaign_dpvp,
            guideline_range,
            guideline_rating_label
        from
            (
                select
                    rating,
                    min_guidelines,
                    max_guidelines,
                    year,
                    guideline_range,
                    guideline_rating_label
                from {{ ref("stg_param_variable_pay_guidelines") }}
                where dbt_valid_to is null
            ) guide
        inner join
            {{ ref("dim_param_variable_pay_campaign") }} camp
            on guide.year = camp.dpvp_variable_pay_campaign_year_dpvp

    ),
    surrogate_key as (
        select p.*, hash(rating, dpvp_pk_variable_pay_campaign_dpvp) as sk_rating
        from guidelines p
    )
select
    sk_rating as dvpg_pk_guidelines_dvpg,
    dpvp_pk_variable_pay_campaign_dpvp as dvpg_fk_variable_pay_campaign_dpvp,
    rating as dvpg_rating_dvpg,
    min_guidelines as dvpg_min_guidelines_dvpg,
    max_guidelines as dvpg_max_guidelines_dvpg,
    guideline_range as dvpg_guideline_range_dvpg,
    guideline_rating_label as dvpg_guideline_rating_label_dvpg
from surrogate_key
union
select '-1', null, null, null, null, null,null
