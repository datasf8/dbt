{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select country_name_en, company_code, ec_go_live_date, ep_go_live_date
from {{ ref("go_live_dates_seed") }}
