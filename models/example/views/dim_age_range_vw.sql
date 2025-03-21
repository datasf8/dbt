select range_code as age_range_code, range as age_range
from {{ ref("dim_range_snapshot") }}
where range_type = 'Age'
