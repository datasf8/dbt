select range_code::int as range_code, range_type as range_type, range as range
from {{ ref('dim_range') }}
