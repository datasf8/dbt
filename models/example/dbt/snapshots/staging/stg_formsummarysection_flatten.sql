{% snapshot stg_formsummarysection_flatten %}
    {{

        config(
          unique_key="FORMDATAID||'-'||FORMCONTENTID",
          strategy='check',
          check_cols='all',
        )

    }}

select distinct
Trim(SRC:"d":"comment"::STRING) AS comment,
Trim(SRC:"d":"commentKey"::STRING) AS commentKey,
Trim(SRC:"d":"commentLabel"::STRING) AS commentLabel,
Trim(SRC:"d":"commentPermission"::STRING) AS commentPermission,
Trim(SRC:"d":"firstName"::STRING) AS firstName,
Trim(SRC:"d":"formContentId"::STRING) AS formContentId,
Trim(SRC:"d":"formDataId"::STRING) AS formDataId,
Trim(SRC:"d":"fullName"::STRING) AS fullName,
Trim(SRC:"d":"itemId"::STRING) AS itemId,
Trim(SRC:"d":"lastName"::STRING) AS lastName,
Trim(SRC:"d":"rating"::STRING) AS rating,
Trim(SRC:"d":"ratingKey"::STRING) AS ratingKey,
Trim(SRC:"d":"ratingLabel"::STRING) AS ratingLabel,
Trim(SRC:"d":"ratingPermission"::STRING) AS ratingPermission,
Trim(SRC:"d":"ratingType"::STRING) AS ratingType,
Trim(SRC:"d":"sectionIndex"::STRING) AS sectionIndex,
Trim(SRC:"d":"textRating"::STRING) AS textRating,
Trim(SRC:"d":"userId"::STRING) AS userId
FROM
{{ ref('stg_formsummarysection') }}
--,lateral flatten ( input => src:d:othersRatingComment:results, OUTER => TRUE)
qualify
            row_number() over (
                partition by FORMDATAID, FORMCONTENTID
                order by DBT_UPDATED_AT desc
            )
            = 1
{% endsnapshot %}