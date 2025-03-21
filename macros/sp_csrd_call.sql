 
{% macro csrd_sp(MONTH_REF, COUNTRY_CODE) %}
    {% set query %}

 CALL HRDP_SDDS_{{ env_var('DBT_REGION') }}_DB.BTDP_DS_C2_H16_CSRD_EU_{{ env_var('DBT_REGION') }}_PRIVATE.SP_CSRD_RECALCULATE_DATA( 
 
 MONTH_REF =>  {{ MONTH_REF}} 
,COUNTRY_CODE =>  '{{ COUNTRY_CODE}}'
 );
        {% endset %}

    {% do run_query(query) %}

{% endmacro %} 