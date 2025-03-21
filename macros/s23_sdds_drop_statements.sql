{% macro s23_sdds_drop_statements() %}

    {% set qPrerequisite %}
        use role HRDP_{{ env_var('DBT_REGION') }}_DOMAIN_ADMIN;
        use database HRDP_CORE_{{ env_var('DBT_REGION') }}_DB;
        use schema CMN_CORE_SCH;
    {% endset %}
    {% do run_query(qPrerequisite) %}

    {% set qRestAll %}
        drop table if exists HRDP_LND_{{ env_var('DBT_REGION') }}_DB.SDDS_LND_SCH.TERMINATION_EVENT_REASONS_SEED;
        drop table if exists HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C2_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.HEADCOUNT;
        drop table if exists HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}_PRIVATE.MOBILITY;
        drop view if exists HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C1_H05_EMPLOYMENT_INFORMATION_EU_{{ env_var('DBT_REGION') }}.MOBILITY_V1;
    {% endset %}
    {% do run_query(qRestAll) %}

{% endmacro %}
