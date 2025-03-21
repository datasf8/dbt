{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}

with
    ratings as (
        select distinct dyer_sk_dyer, dyer_code_dyer, dyer_label_dyer
        from {{ source("landing_tables_PMG", "YE_FORM_RATINGS") }}

    )

select dyer_sk_dyer, dyer_code_dyer, dyer_label_dyer
from ratings
