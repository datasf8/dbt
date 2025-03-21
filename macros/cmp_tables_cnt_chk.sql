{% macro cmp_tables_cnt_chk(table_name) -%}
    {%- set result -%}
        select count(*) from hrdp_lnd_{{ env_var("DBT_ENV_NAME") }}_db.CMP_LND_SCH.{{ table_name }}
    {%- endset -%}

    {% if execute and flags.WHICH in [
        "run",
        "build",
        "test",
        "seed",
        "run-operation",
        "snapshot",
    ] %}
        {%- set load_result_output = run_query(result) -%}
        {%- set file_count = load_result_output.columns[0].values()[0] | int -%}
        {{ log("count : " ~ file_count) }}
        {%- if file_count == 0 -%}
            {{ exceptions.raise_compiler_error("!!!!! No rows in the table") }}
        {% endif %}
    {% endif %}
    {{ log("Table_cnt_status: " ~ read_status) }}

    {{ return(read_status) }}
{% endmacro %}