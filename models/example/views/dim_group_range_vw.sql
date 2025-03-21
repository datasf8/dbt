select
    range_code as range_code,
    range as range,
    collate(
        case when range_code = 9 then 'New Comers Only' else 'Without New Comers' end,
        'en-ci'
    ) as in_nc
from {{ ref("dim_range_snapshot") }}
where range_type = 'Group'
