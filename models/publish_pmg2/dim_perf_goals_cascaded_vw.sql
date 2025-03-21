select distinct
    dpgs_perf_goals_cascaded_sk_dpgs as goal_cascaded_key,
    dpgs_perf_goals_cascaded_key_dpgs as goal_cascaded_key_v1,
    dpgs_perf_goals_cascaded_label_dpgs as goal_cascaded,
    case
        when dpgs_perf_goals_cascaded_label_dpgs = 'Aligned'
        then 'Cascaded'
        when dpgs_perf_goals_cascaded_label_dpgs = 'Unaligned'
        then 'Self-assigned'
        else dpgs_perf_goals_cascaded_label_dpgs
    end as goal_cascaded_label

from {{ ref("dim_perf_goals_cascaded") }}
