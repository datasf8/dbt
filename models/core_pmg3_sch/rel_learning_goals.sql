{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
with
    learning_goals as (
        select distinct ddll_linked_learning_sk_ddll, ddpg_dev_goal_sk_ddpg
        from (select * from {{ ref("dim_linked_learning") }}) dll
        inner join
            (
                select *
                from {{ ref("stg_dev_goal_report") }}
                where learning_activity_id <> ''
            ) sdgr
            on dll.ddll_linked_id_ddll = sdgr.learning_activity_id
        inner join
            {{ ref("dim_dev_goals") }} ddg
            on sdgr.development_goal_id = ddg.ddpg_dev_goal_id_ddpg
    )

select
    ddll_linked_learning_sk_ddll as rdlg_linked_learning_key_ddll,
    ddpg_dev_goal_sk_ddpg as rdlg_dev_goal_key_ddpg
from learning_goals
