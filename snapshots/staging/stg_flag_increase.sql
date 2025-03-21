{% snapshot stg_flag_increase %}
    {{
        config(
          unique_key="CODE_ID_INCREASE",
          strategy='check',
          check_cols='all'
)

    }}
select * from
{{ ref('flag_increase') }}
{% endsnapshot %}