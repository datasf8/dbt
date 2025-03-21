select
    range_code as range_payout_code,
    range as payout_range,
    range_inf as payout_range_inf,
    range_sup as payout_range_sup
from {{ ref('dim_range_snapshot') }}
where range_type = 'Payout'
