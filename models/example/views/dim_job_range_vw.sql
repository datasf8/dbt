select range_code as range_code, range as range
from {{ ref("dim_range_snapshot") }}
where range_type = 'Job'
