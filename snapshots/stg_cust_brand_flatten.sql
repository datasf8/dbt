
{% snapshot stg_cust_brand_flatten %}
    {{
        config(
          unique_key= "externalCode||'-'||effectiveStartDate",
          strategy='check',
          check_cols='all',
          target_schema= "SDDS_STG_SCH",
          invalidate_hard_deletes=true
        )
    }}

select 
Trim(VALUE:"externalCode"::STRING) AS externalCode,
to_date(DATEADD(MS, replace(split_part(value:"effectiveStartDate", '(', 2), ')/', ''), '1970-01-01')) AS effectiveStartDate,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDateTime,
to_date(DATEADD(MS, replace(split_part(value:"mdfSystemEffectiveEndDate", '(', 2), ')/', ''), '1970-01-01')) AS mdfSystemEffectiveEndDate,
DATEADD(MS, replace(split_part(split_part(value:"createdDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS createdDateTime,
Trim(VALUE:"externalName_defaultValue"::STRING) AS externalName_defaultValue,
Trim(VALUE:"externalName_es_MX"::STRING) AS externalName_es_MX,
Trim(VALUE:"externalName_zh_TW"::STRING) AS externalName_zh_TW,
Trim(VALUE:"externalName_ja_JP"::STRING) AS externalName_ja_JP,
Trim(VALUE:"mdfSystemStatus"::STRING) AS mdfSystemStatus,
Trim(VALUE:"externalName_pt_BR"::STRING) AS externalName_pt_BR,
Trim(VALUE:"externalName_pl_PL"::STRING) AS externalName_pl_PL,
Trim(VALUE:"externalName_ru_RU"::STRING) AS externalName_ru_RU,
Trim(VALUE:"externalName_it_IT"::STRING) AS externalName_it_IT,
Trim(VALUE:"lastModifiedBy"::STRING) AS lastModifiedBy,
Trim(VALUE:"externalName_zh_CN"::STRING) AS externalName_zh_CN,
Trim(VALUE:"externalName_localized"::STRING) AS externalName_localized,
Trim(VALUE:"mdfSystemRecordStatus"::STRING) AS mdfSystemRecordStatus,
Trim(VALUE:"externalName_bs_ID"::STRING) AS externalName_bs_ID,
Trim(VALUE:"externalName_fr_FR"::STRING) AS externalName_fr_FR,
Trim(VALUE:"externalName_de_DE"::STRING) AS externalName_de_DE,
Trim(VALUE:"externalName_ko_KR"::STRING) AS externalName_ko_KR,
Trim(VALUE:"createdBy"::STRING) AS createdBy,
Trim(VALUE:"externalName_en_US"::STRING) AS externalName_en_US,
Trim(VALUE:"externalName_en_GB"::STRING) AS externalName_en_GB
from {{ ref('stg_cust_brand') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by externalCode, effectiveStartDate
                order by lastModifiedDateTime desc
            )
            = 1
{% endsnapshot %}
