select distinct
    dpgs_perf_goals_status_sk_dpgs as goal_status_key,
    dpgs_perf_goals_status_key_dpgs as goal_status_key_v1,
    dpgs_perf_goals_status_label_dpgs as goal_status_label
from {{ ref("dim_perf_goals_status") }}
