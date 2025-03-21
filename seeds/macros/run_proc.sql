 
{% macro run_proc() %}
    {% set query %}

 CALL {{ env_var('DBT_QCK_DB') }}.{{ env_var('DBT_QCK_SCH') }}.ALIM_DATA_QUALITY( 
 
 DB => '{{ env_var('DBT_QCK_DB') }}'
,SCH => '{{ env_var('DBT_QCK_SCH') }}'
 );
        {% endset %}

    {% do run_query(query) %}

     {% set query2 %}   
 CALL {{ env_var('DBT_QCK_DB') }}.{{ env_var('DBT_QCK_SCH') }}.ALIM_DIM_PARAM_DATAQUALITY_QUERIES_FIELDS_ORDER();
   {% endset %}
    {% do run_query(query2) %}  

{% endmacro %} 