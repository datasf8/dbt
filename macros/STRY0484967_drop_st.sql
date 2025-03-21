{% macro STRY0484967_drop_st() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB;
        use schema BTDP_DS_C1_H03_REWARDS_AND_PAY_STRUCTURE_EU_{{ env_var('DBT_REGION') }};
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}

        drop View HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H03_REWARDS_AND_PAY_STRUCTURE_EU_{{ env_var('DBT_REGION') }}.PAY_COMPONENT_MAPPING_V2;


    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
