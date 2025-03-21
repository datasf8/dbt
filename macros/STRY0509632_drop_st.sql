{% macro STRY0509632_drop_st() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB;
        use schema BTDP_DS_C1_H19_HR_CONTROLING_EU_{{ env_var('DBT_REGION') }};
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        drop TABLE HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H19_HR_CONTROLING_EU_{{ env_var('DBT_REGION') }}_PRIVATE.FACT_ADOPTION_AGG;
        drop View  HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H19_HR_CONTROLING_EU_{{ env_var('DBT_REGION') }}.FACT_ADOPTION_AGG_V1 ;


    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
