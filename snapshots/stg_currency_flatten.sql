
{% snapshot stg_currency_flatten %}
    {{
        config(
          unique_key= "code||'-'||effectiveStartDate",
          strategy='check',
          check_cols='all',
          target_schema= "SDDS_STG_SCH",
          invalidate_hard_deletes=true
        )
    }}

select 
Trim(VALUE:"code"::STRING) AS code,
to_date(DATEADD(MS, replace(split_part(value:"effectiveStartDate", '(', 2), ')/', ''), '1970-01-01')) AS effectiveStartDate,
Trim(VALUE:"symbol"::STRING) AS symbol,
Trim(VALUE:"description_ko_KR"::STRING) AS description_ko_KR,
DATEADD(MS, replace(split_part(split_part(value:"lastModifiedDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS lastModifiedDateTime,
DATEADD(MS, replace(split_part(split_part(value:"createdDateTime", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS createdDateTime,
Trim(VALUE:"description_pt_BR"::STRING) AS description_pt_BR,
Trim(VALUE:"externalName_zh_TW"::STRING) AS externalName_zh_TW,
Trim(VALUE:"externalName_ja_JP"::STRING) AS externalName_ja_JP,
Trim(VALUE:"description_es_MX"::STRING) AS description_es_MX,
Trim(VALUE:"externalName_pl_PL"::STRING) AS externalName_pl_PL,
Trim(VALUE:"externalName_zh_CN"::STRING) AS externalName_zh_CN,
Trim(VALUE:"externalName_localized"::STRING) AS externalName_localized,
Trim(VALUE:"mdfSystemRecordStatus"::STRING) AS mdfSystemRecordStatus,
Trim(VALUE:"description_defaultValue"::STRING) AS description_defaultValue,
Trim(VALUE:"externalName_bs_ID"::STRING) AS externalName_bs_ID,
Trim(VALUE:"externalName_fr_FR"::STRING) AS externalName_fr_FR,
Trim(VALUE:"description_en_US"::STRING) AS description_en_US,
Trim(VALUE:"externalName_en_GB"::STRING) AS externalName_en_GB,
Trim(VALUE:"description_it_IT"::STRING) AS description_it_IT,
Trim(VALUE:"description_ru_RU"::STRING) AS description_ru_RU,
Trim(VALUE:"status"::STRING) AS status,
to_date(DATEADD(MS, replace(split_part(value:"effectiveEndDate", '(', 2), ')/', ''), '1970-01-01')) AS effectiveEndDate,
Trim(VALUE:"description_ja_JP"::STRING) AS description_ja_JP,
Trim(VALUE:"description_fr_FR"::STRING) AS description_fr_FR,
Trim(VALUE:"externalName_defaultValue"::STRING) AS externalName_defaultValue,
Trim(VALUE:"description_de_DE"::STRING) AS description_de_DE,
Trim(VALUE:"externalName_es_MX"::STRING) AS externalName_es_MX,
Trim(VALUE:"externalName_pt_BR"::STRING) AS externalName_pt_BR,
Trim(VALUE:"description_zh_TW"::STRING) AS description_zh_TW,
Trim(VALUE:"description_bs_ID"::STRING) AS description_bs_ID,
Trim(VALUE:"description_en_GB"::STRING) AS description_en_GB,
Trim(VALUE:"externalName_ru_RU"::STRING) AS externalName_ru_RU,
Trim(VALUE:"externalName_it_IT"::STRING) AS externalName_it_IT,
Trim(VALUE:"lastModifiedBy"::STRING) AS lastModifiedBy,
Trim(VALUE:"defaultDecimals"::STRING) AS defaultDecimals,
Trim(VALUE:"externalName_de_DE"::STRING) AS externalName_de_DE,
Trim(VALUE:"externalName_ko_KR"::STRING) AS externalName_ko_KR,
Trim(VALUE:"description_zh_CN"::STRING) AS description_zh_CN,
Trim(VALUE:"createdBy"::STRING) AS createdBy,
Trim(VALUE:"description_localized"::STRING) AS description_localized,
Trim(VALUE:"externalName_en_US"::STRING) AS externalName_en_US,
Trim(VALUE:"description_pl_PL"::STRING) AS description_pl_PL
from {{ ref('stg_currency') }}
,lateral flatten (input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 
qualify
            row_number() over (
                partition by code, effectiveStartDate
                order by lastModifiedDateTime desc
            )
            = 1
{% endsnapshot %}
