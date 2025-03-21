{% snapshot stg_position_in_range %}
    {{
        config(
          unique_key="POSITION_IN_RANGE_CODE",
          strategy='check',
          check_cols='all'
)

    }}
select * from
{{ ref('position_in_range') }}
{% endsnapshot %}