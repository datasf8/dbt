select distinct dpfy_year_dpfy as year from {{ ref("dim_param_perf_goals_years") }}
