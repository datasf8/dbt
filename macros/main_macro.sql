{% macro  my_macro() %}
    {# use a statement block #}
    {% call statement('films', fetch_result=True, auto_begin=True) %}
        {# sql query is wrapped in a macro  #}
        {{ create_grant_db_lrn() }}
         
    {% endcall %}

{% endmacro %}