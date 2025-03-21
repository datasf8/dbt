{% macro STRY0380172_pmgm_sk_alter_stat() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.CMN_CORE_SCH.rel_employee_managers
        RENAME COLUMN REEM_EMPLOYEE_PROFILE_SK_DDEP TO REEM_EMPLOYEE_PROFILE_SK_DDEP_COPY  ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals
        RENAME COLUMN FDPG_EMPLOYEE_PROFILE_KEY_DDEP  TO FDPG_EMPLOYEE_PROFILE_KEY_DDEP_COPY   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals
        RENAME COLUMN fdpg_dev_goal_key_ddpg  TO fdpg_dev_goal_key_ddpg_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals
        RENAME COLUMN fdpg_dev_goals_status_key_ddgs  TO fdpg_dev_goals_status_key_ddgs_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals
        RENAME COLUMN fdpg_dev_goal_category_key_ddgc  TO fdpg_dev_goal_category_key_ddgc_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals
        RENAME COLUMN fdpg_purpose_key_ddpp  TO fdpg_purpose_key_ddpp_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update
        RENAME COLUMN FDGU_EMPLOYEE_PROFILE_KEY_DDEP   TO FDGU_EMPLOYEE_PROFILE_KEY_DDEP_COPY    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update
        RENAME COLUMN fdgu_dev_goal_key_ddpg  TO fdgu_dev_goal_key_ddpg_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update
        RENAME COLUMN fdgu_dev_goals_status_key_ddgs  TO fdgu_dev_goals_status_key_ddgs_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update
        RENAME COLUMN fdgu_dev_goal_category_key_ddgc  TO fdgu_dev_goal_category_key_ddgc_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update
        RENAME COLUMN fdgu_purpose_key_ddpp  TO fdgu_purpose_key_ddpp_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_final
        RENAME COLUMN fdpg_employee_profile_key_ddep   TO fdpg_employee_profile_key_ddep_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_final
        RENAME COLUMN fdpg_dev_goal_key_ddpg  TO fdpg_dev_goal_key_ddpg_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_final
        RENAME COLUMN fdpg_dev_goals_status_key_ddgs  TO fdpg_dev_goals_status_key_ddgs_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_final
        RENAME COLUMN fdpg_dev_goal_category_key_ddgc  TO fdpg_dev_goal_category_key_ddgc_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_final
        RENAME COLUMN fdpg_purpose_key_ddpp  TO fdpg_purpose_key_ddpp_copy   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update_final
        RENAME COLUMN FDGU_EMPLOYEE_PROFILE_KEY_DDEP   TO FDGU_EMPLOYEE_PROFILE_KEY_DDEP_COPY    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update_final
        RENAME COLUMN FDGU_DEV_GOAL_KEY_DDPG  TO FDGU_DEV_GOAL_KEY_DDPG_COPY   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update_final
        RENAME COLUMN FDGU_DEV_GOALS_STATUS_KEY_DDGS  TO FDGU_DEV_GOALS_STATUS_KEY_DDGS_COPY   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update_final
        RENAME COLUMN FDGU_DEV_GOAL_CATEGORY_KEY_DDGC  TO FDGU_DEV_GOAL_CATEGORY_KEY_DDGC_COPY   ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_dev_goals_update_final
        RENAME COLUMN FDGU_PURPOSE_KEY_DDPP  TO FDGU_PURPOSE_KEY_DDPP_COPY   ;
        
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals
        RENAME COLUMN FPPG_EMPLOYEE_KEY_DDEP   TO FPPG_EMPLOYEE_KEY_DDEP_COPY    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals
        RENAME COLUMN fppg_perf_goal_key_dppg   TO fppg_perf_goal_key_dppg_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals
        RENAME COLUMN fppg_category_key_dpgc   TO fppg_category_key_dpgc_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals
        RENAME COLUMN fppg_perf_goals_cascaded_key_dpgs   TO fppg_perf_goals_cascaded_key_dpgs_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals
        RENAME COLUMN fppg_perf_goals_type_key_dpgt   TO fppg_perf_goals_type_key_dpgt_copy    ;
        
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals
        RENAME COLUMN fppg_status_dpgs   TO fppg_status_dpgs_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_final
        RENAME COLUMN fppg_employee_key_ddep   TO fppg_employee_key_ddep_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_final
        RENAME COLUMN fppg_perf_goal_key_dppg   TO fppg_perf_goal_key_dppg_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_final
        RENAME COLUMN fppg_status_dpgs   TO fppg_status_dpgs_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_final
        RENAME COLUMN fppg_category_key_dpgc   TO fppg_category_key_dpgc_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_final
        RENAME COLUMN fppg_perf_goals_cascaded_key_dpgs   TO fppg_perf_goals_cascaded_key_dpgs_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_final
        RENAME COLUMN fppg_perf_goals_type_key_dpgt   TO fppg_perf_goals_type_key_dpgt_copy    ;
        
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_update
        RENAME COLUMN fpgu_perf_goal_key_dppg   TO fpgu_perf_goal_key_dppg_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_update_final
        RENAME COLUMN FPGU_PERF_GOAL_KEY_DPPG   TO FPGU_PERF_GOAL_KEY_DPPG_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_update_final
        RENAME COLUMN FPPG_EMPLOYEE_KEY_DDEP   TO FPPG_EMPLOYEE_KEY_DDEP_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_update_final
        RENAME COLUMN FPPG_CATEGORY_KEY_DPGC   TO FPPG_CATEGORY_KEY_DPGC_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_update_final
        RENAME COLUMN FPPG_STATUS_DPGS   TO FPPG_STATUS_DPGS_copy    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_update_final
        RENAME COLUMN FPPG_PERF_GOALS_CASCADED_KEY_DPGS   TO FPPG_PERF_GOALS_CASCADED_KEY_DPGS_COPY    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_perf_goals_update_final
        RENAME COLUMN FPPG_PERF_GOALS_TYPE_KEY_DPGT   TO FPPG_PERF_GOALS_TYPE_KEY_DPGT_COPY    ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.rel_activities_goals
        RENAME COLUMN REAG_ACTIVITY_KEY_DCAC TO REAG_ACTIVITY_KEY_DCAC_COPY ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.fact_ye_form_v2 
        RENAME COLUMN FYEF_EMPLOYEE_PROFILE_KEY_DDEP TO FYEF_EMPLOYEE_PROFILE_KEY_DDEP_COPY;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.rel_activities_goals
        RENAME COLUMN REAG_GOAL_KEY_REAG  TO REAG_GOAL_KEY_REAG_COPY  ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.rel_dev_activities_goals
        RENAME COLUMN reag_activity_key_dcac TO reag_activity_key_dcac_copy ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.rel_dev_activities_goals
        RENAME COLUMN reag_goal_key_dppg  TO reag_goal_key_dppg_copy  ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.rel_learning_goals  
        RENAME COLUMN rdlg_linked_learning_key_ddll TO rdlg_linked_learning_key_ddll_copy ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.rel_learning_goals  
        RENAME COLUMN rdlg_dev_goal_key_ddpg  TO rdlg_dev_goal_key_ddpg_copy  ;    
      
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.dim_career_aspirations  
        RENAME COLUMN dcra_employee_profile_key_ddep TO dcra_employee_profile_key_ddep_copy ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.dim_career_recommendations_hr  
        RENAME COLUMN dcrh_employee_profile_key_ddep  TO dcrh_employee_profile_key_ddep_copy  ;
        
        ALTER TABLE HRDP_CORE_{{ env_var('DBT_REGION') }}_DB.PMG_CORE_SCH.dim_career_recommendations_managers  
        RENAME COLUMN dcrm_employee_profile_key_ddep  TO dcrm_employee_profile_key_ddep_copy  ;
        
    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
