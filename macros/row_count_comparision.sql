{% macro test_equal_rowcount(model, compare_model) -%}

    with
        a as (

            select 1 as id_dbtutils_test_equal_rowcount, count(*) as count_a
            from {{ model }}

        ),
        b as (

            select 1 as id_dbtutils_test_equal_rowcount, count(*) as count_b
            from {{ compare_model }}

        ),
        final as (

            select (select count_a from a) = (select count_b from b) as row_count_result

        )

    select *
    from final

{%- endmacro %}
