{{
    config(
        materialized="view",
        post_hook="  alter view {{ env_var('DBT_PUB_DB') }}.PMG_PUB_SCH.DIM_DEV_GOALS_VW  modify column GOAL_NAME set masking policy {{ env_var('DBT_CORE_DB') }}.CMN_CORE_SCH.PMGM_RATING_COL_MASKING  using (GOAL_NAME, EMP_ID); ",
    )
}}
select distinct
    ddpg_dev_goal_sk_ddpg as goal_key,
    ddpg_dev_goal_key_ddpg as goal_key_v1,
    ddpg_user_id_ddpg as emp_id,
    ddpg_dev_goal_id_ddpg as goal_id,
    ddpg_dev_goal_name_ddpg as goal_name,
    ddpg_goal_owner_ddpg as goal_owner,
    ddpg_goal_modifier_ddpg as goal_modifier
from {{ ref("dim_dev_goals") }}
