select distinct
    ddgc_dev_goal_category_sk_ddgc as goal_category_key,
    ddgc_dev_goal_category_key_ddgc as goal_category_key_v1,
    ddgc_dev_goal_category_label_ddgc as goal_category_label
from {{ ref("dim_dev_goals_category") }}
