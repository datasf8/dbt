select distinct
    ddpp_purpose_sk_ddpp as purpose_key,
    ddpp_purpose_key_ddpp as purpose_key_v1,
    ddpp_purpose_ddpp as purpose,
    ddpp_purpose_label_ddpp as purpose_label
from {{ ref("dim_goal_purpose") }}
