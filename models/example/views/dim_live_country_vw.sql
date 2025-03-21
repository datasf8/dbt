select org_code as org_code, org_label as org_label
from {{ ref("dim_live_country_snapshot") }}
