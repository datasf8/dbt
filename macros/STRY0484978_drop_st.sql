{% macro STRY0484978_drop_st() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB;
        use schema BTDP_DS_C2_H03_REWARDS_AND_PAY_STRUCTURE_EU_{{ env_var('DBT_REGION') }}_PRIVATE;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        drop Table HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C2_H03_REWARDS_AND_PAY_STRUCTURE_EU_{{ env_var('DBT_REGION') }}_PRIVATE.PAY_RECURRING ;

        drop View  HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C2_H03_REWARDS_AND_PAY_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.PAY_RECURRING_V1 ;

    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
