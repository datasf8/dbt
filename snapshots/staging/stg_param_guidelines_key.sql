{% snapshot stg_param_guidelines_key %}
    {{
        config(
          unique_key="GUIDELINE_CODE",
          strategy='check',
          check_cols='all',
)

    }}
select * from
{{ ref('param_guidelines_key') }}
{% endsnapshot %}
