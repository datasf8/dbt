select distinct
    dpgc_perf_goal_category_sk_dpgc as goal_category_key,
    dpgc_perf_goal_category_key_dpgc as goal_category_key_v1,
    case
        when dpgc_perf_goal_category_label_dpgc = 'Individual Goals'
        then 'Business Goals'
        else dpgc_perf_goal_category_label_dpgc
    end as goal_category_label
from {{ ref("dim_perf_goals_category") }}
