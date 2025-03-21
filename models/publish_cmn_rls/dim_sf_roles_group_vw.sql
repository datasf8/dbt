select distinct sfroleid as sf_role_id, grtgroup as grt_group, tgtgroup as tgt_group
from {{ ref("dim_sf_roles_group") }}
