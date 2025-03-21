{% macro STRY0480286_drop_st() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_PUB_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_PUB_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}
    
        drop TABLE HRDP_LND_{{ env_var('DBT_REGION') }}_DB.SDDS_LND_SCH.PARAM_LEAVE_CATEGORY;
        --drop TABLE HRDP_PUB_{{ env_var('DBT_REGION') }}_DB.CMN_PUB_SCH.DIM_PARAM_LEAVE_CATEGORY;  

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
