{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    ethnicity as (
        select
            ethnicity_id::int as ethnicity_id,
            ethnicity_code,
            ethnicity_name_en,
            ethnicity_status
        from {{ ref("ethnicity") }}
        union all
        select -1, null, null, null
    ),
    race as (
        select race_id::int as race_id, race_code, race_name_en, race_status
        from {{ ref("race") }}
        union all
        select -1, null, null, null
    )
select
    hash(ethnicity_id, r.race_id) as ethnicity_sk,
    et.*,
    iff(
        et.ethnicity_code = 'HL',  -- HL: Hispainic or LatinX
        et.ethnicity_name_en,
        iff(r.race_name_en = 'No Selection', 'Prefer Not to Say', r.race_name_en)
    ) ethnicity_name_label,
    r.*,
    iff(et.ethnicity_code = 'HL', null, r.race_name_en) as race_name_label
from ethnicity et
join race r
