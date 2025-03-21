{% snapshot stg_flag_conformed_to_guidance %}
    {{
        config(
          unique_key="CODE_ID_INCREASE",
          strategy='check',
          check_cols='all'
)

    }}
select * from
{{ ref('flag_conformed_to_guidance') }}
{% endsnapshot %}