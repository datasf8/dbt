{% snapshot stg_param_variable_pay_campaign %}
    {{
        config(
          unique_key="VARIABLE_PAY_CODE",
          strategy='check',
          check_cols='all',
)

    }}
select * from
{{ ref('param_variable_pay_campaign') }}
{% endsnapshot %}
