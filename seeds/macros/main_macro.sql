{% macro  main_macro() %}
    {# use a statement block #}
 
        {# sql query is wrapped in a macro  #}
        {{ ALIM_DIM_ORGANISATION() }}
        {{ ALIM_DIM_PROFESSIONAL_FIELD() }}     
        {{ ALIM_LOG_DATAQUALITY_RUNS() }}     
        {{ ALIM_LOG_DATA_QUALITY_BY_RULE() }}     
        {{ ALIM_FACT_DATAQUALITY_RESULTS() }}     
        {{ ALIM_DATA_QUALITY() }}     
		{{ ALIM_DIM_PARAM_DATAQUALITY_QUERIES_FIELDS_ORDER() }}
        {{ SP_SQL_EXECUTE_STATEMENT() }}  

      {#   {{ ALIM_DIM_PARAM_DATAQUALITY_QUERIES() }}   #}
 

{% endmacro %}