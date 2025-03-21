{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    to_date(
        position_indicators_referential_start_date::string, 'yyyymmdd'
    ) as position_indicators_referential_start_date,
    to_date(
        position_indicators_referential_end_date::string, 'yyyymmdd'
    ) as position_indicators_referential_end_date,
    hash(
        position_indicators_referential_start_date,
        position_indicators_code
    ) as position_indicators_referential_id,
    position_indicators_code,
    position_indicators_name,
    position_indicators_sort,
    position_indicators_group_code,
    position_indicators_group_name,
    position_indicators_group_sort,
    position_indicators_subgroup_code,
    position_indicators_subgroup_name,
    position_indicators_subgroup_sort
from {{ ref("position_indicators_referential_seed") }}
