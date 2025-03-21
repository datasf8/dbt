select distinct
    ddgs_dev_goals_status_sk_ddgs as goal_status_key,
    ddgs_dev_goals_status_key_ddgs as goal_status_key_v1,
    ddgs_dev_goals_status_label_ddgs as goal_status_label
from {{ ref("dim_dev_goals_status") }}
