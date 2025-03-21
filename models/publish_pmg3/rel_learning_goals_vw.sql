select distinct
    rdlg_linked_learning_key_ddll as linked_learning_key,
    rdlg_dev_goal_key_ddpg as goal_key
from {{ ref("rel_learning_goals") }}
