select distinct
    reag_activity_key_dcac as activity_key,
    reag_goal_key_dppg as goal_key,
    reag_goal_type_reag as goal_type
from {{ ref("rel_dev_activities_goals") }}
