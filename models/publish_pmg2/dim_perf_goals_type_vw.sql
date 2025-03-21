select distinct
    dpgt_perf_goals_type_sk_dpgt as goal_type_key,
    dpgt_perf_goals_type_key_dpgt as goal_type_key_v1,
    dpgt_perf_goals_type_label_dpgt as goal_type_label,
    perf_goals_characteristic as goals_characteristic
from {{ ref("dim_perf_goals_type") }}
