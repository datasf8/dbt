{{ config(materialized='table',transient=false) }}

WITH USER_DETAILS 
AS
(
select 
  USER_ID,
  GOAL_ID,
  GOAL_NAME,
  KPI_EXPECTED_RESULTS,
  CATEGORY,
  STATUS,
  LAST_MODIFIED_DATE,
  GOAL_CHARACTERISTIC,
  DUE_DATE,
  COUNT(Distinct GOAL_ID) Over (partition by USER_ID)  as NB_GOALS_PER_USER,
  GOAL_TEMPLATE_ID
  from (select * from {{ ref('stg_user_goal_details_flatten') }} where DBT_VALID_TO is null)
)
SELECT
  USER_ID, 
  GOAL_ID,
  GOAL_NAME,
  KPI_EXPECTED_RESULTS,
  CATEGORY,
  STATUS,
  LAST_MODIFIED_DATE,
  GOAL_CHARACTERISTIC,
  DUE_DATE,
  NB_GOALS_PER_USER,
  GOAL_TEMPLATE_ID
  FROM USER_DETAILS