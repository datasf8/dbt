{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    guidelines_key as (

        select guideline_code, people_guidelines_aligned, team_guidelines_aligned
        from {{ ref('stg_param_guidelines_key') }} )

        select *
        from guidelines_key
