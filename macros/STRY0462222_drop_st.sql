{% macro STRY0462222_drop_st() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_STG_{{ env_var('DBT_REGION') }}_DB;
        use schema SDDS_STG_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        drop TABLE HRDP_STG_{{ env_var('DBT_REGION') }}_DB.SDDS_STG_SCH.STG_CUST_US_JOBCODE;

        drop TABLE HRDP_STG_{{ env_var('DBT_REGION') }}_DB.SDDS_STG_SCH.STG_CUST_US_JOBCODE_FLATTEN;    

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
