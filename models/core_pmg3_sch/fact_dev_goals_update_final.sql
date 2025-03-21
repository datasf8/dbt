{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
WITH FACT_DEV_UPDATE AS (
        SELECT DISTINCT FDGU_EMPLOYEE_PROFILE_KEY_DDEP
            ,FDGU_DEV_GOAL_KEY_DDPG
            ,FDGU_DEV_GOALS_STATUS_KEY_DDGS
            ,FDGU_PURPOSE_KEY_DDPP
            ,FDGU_DEV_GOAL_CATEGORY_KEY_DDGC
            ,FDGU_IS_CURRENT_SITUATION_FDGU
            ,FDGU_PREVIOUS_GOAL_NAME_FDGU
            ,FDGU_UPDATED_GOAL_NAME_FDGU
            ,FDGU_UPDATE_DATE_FDGU
            ,COLLATE(CASE 
                WHEN ddgp_year_ddgp = YEAR(FDGU_UPDATE_DATE_FDGU)
                    THEN RIGHT('0' || Cast(MONTH(FDGU_UPDATE_DATE_FDGU) AS VARCHAR(4)), 2)
                WHEN ddgp_year_ddgp > YEAR(FDGU_UPDATE_DATE_FDGU)
                    THEN '.Prev Years'
                WHEN ddgp_year_ddgp < YEAR(FDGU_UPDATE_DATE_FDGU)
                    THEN 'Next Years'
                END,'en-ci') AS UPDATE_DATE_MONTH_DISPLAY
            ,FDGU_dev_goal_template_id_DDGP
            ,FDGU_YEAR_DDGP

        FROM {{ ref('fact_dev_goals_update') }}
        INNER JOIN {{ ref('fact_dev_goals') }} ON FDGU_DEV_GOAL_KEY_DDPG = FDPG_DEV_GOAL_KEY_DDPG
        INNER JOIN {{ ref('dim_dev_goals_plan') }} DDGP ON DEV_GOAL_PLAN_TEMPLATE_ID  = ddgp_dev_goal_template_id_ddgp
        INNER JOIN {{ ref('dim_dev_goals_category') }} DDGC ON ddgc_dev_goal_category_sk_ddgc = FDPG_DEV_GOAL_CATEGORY_KEY_DDGC
        WHERE (FDGU_IS_CURRENT_SITUATION_FDGU = 'Active' or FDGU_IS_CURRENT_SITUATION_FDGU is null)
        AND DDGC_DEV_GOAL_CATEGORY_LABEL_DDGC <> 'Archive'
        )

SELECT FDGU_EMPLOYEE_PROFILE_KEY_DDEP
    ,FDGU_DEV_GOAL_KEY_DDPG
    ,FDGU_DEV_GOALS_STATUS_KEY_DDGS
    ,FDGU_PURPOSE_KEY_DDPP
    ,FDGU_DEV_GOAL_CATEGORY_KEY_DDGC
    ,FDGU_IS_CURRENT_SITUATION_FDGU
    ,FDGU_PREVIOUS_GOAL_NAME_FDGU
    ,FDGU_UPDATED_GOAL_NAME_FDGU
    ,FDGU_UPDATE_DATE_FDGU
    ,UPDATE_DATE_MONTH_DISPLAY
    ,FDGU_dev_goal_template_id_DDGP
    ,FDGU_YEAR_DDGP
FROM FACT_DEV_UPDATE