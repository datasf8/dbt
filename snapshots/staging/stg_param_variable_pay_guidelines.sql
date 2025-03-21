{% snapshot stg_param_variable_pay_guidelines %}
    {{
        config(
          unique_key="RATING||'-'||YEAR",
          strategy='check',
          check_cols='all',
)

    }}
select * from
{{ ref('param_variable_pay_guidelines') }}
{% endsnapshot %}
