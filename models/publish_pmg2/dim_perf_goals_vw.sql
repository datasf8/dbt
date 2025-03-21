{{ 
  config
  (
    materialized='view',
post_hook = "  
alter view {{ env_var('DBT_PUB_DB') }}.PMG_PUB_SCH.DIM_PERF_GOALS_VW 
modify column GOAL_NAME set masking policy {{ env_var('DBT_CORE_DB') }}.CMN_CORE_SCH.PMGM_RATING_COL_MASKING  
using (GOAL_NAME, EMP_ID);
alter view {{ env_var('DBT_PUB_DB') }}.PMG_PUB_SCH.DIM_PERF_GOALS_VW 
modify column GOAL_KPI set masking policy {{ env_var('DBT_CORE_DB') }}.CMN_CORE_SCH.PMGM_RATING_COL_MASKING  
using (GOAL_KPI, EMP_ID);
"
)
  }}

select distinct
    dppg_perf_goal_sk_dppg as goal_key,
    dppg_perf_goal_key_dppg as goal_key_v1,
    dppg_employee_id_dppg as emp_id,
    dppg_perf_goal_id_dppg as goal_id,
    collate(substr(dppg_perf_goal_name_dppg, 1, 100), 'en-ci') as goal_name,
    collate(substr(dppg_perf_goal_kpi_dppg, 1, 100), 'en-ci') as goal_kpi,
    dppg_perf_goal_name_data_quality_dppg as goal_name_data_quality,
    dppg_perf_goal_kpi_data_quality_dppg as goal_kpi_data_quality
from {{ ref("dim_perf_goals") }}
