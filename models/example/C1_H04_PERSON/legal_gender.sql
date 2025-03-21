{{
    config(
        materialized="table",
        transient=false,
    )
}}
select hash(legal_gender_code, legal_gender_start_date) legal_gender_id, *
from {{ ref("legal_gender_seed") }}
