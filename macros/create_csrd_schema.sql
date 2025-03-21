{% macro create_csrd_schema() %}
    {% set query %}

 CALL HRDP_ADM_{{ env_var('DBT_REGION') }}_DB.DB_SCHEMA_SCH.CREATE_SCHMEA_SP('HRDP_PUB_{{ env_var('DBT_REGION') }}_DB', 'CSRD_PUB_SCH');

    {% endset %}

    {% do run_query(query) %}

{% endmacro %}
