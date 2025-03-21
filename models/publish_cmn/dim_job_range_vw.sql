select range_code as range_code, range as range
from {{ ref("dim_range") }}
where range_type = 'Job'
order by 1
