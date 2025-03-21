{% snapshot stg_campaign %}
    {{
        config(
          unique_key="CAMPAIGN_CODE",
          strategy='check',
          check_cols='all'
)

    }}
select * from
{{ ref('campaign') }} where ENV = '{{ env_var("DBT_ENV_NAME") }}'
{% endsnapshot %}