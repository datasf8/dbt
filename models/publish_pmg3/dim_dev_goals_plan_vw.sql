select distinct
    ddgp_dev_goal_template_id_ddgp as dev_goal_template_id,
    ddgp_dev_goal_plan_name_ddgp as dev_goal_plan_name,
    ddgp_year_ddgp as year,
    ddgp_display_order_ddgp as display_order,
    ddgp_due_date_ddgp as due_date
from {{ ref("dim_dev_goals_plan") }}
