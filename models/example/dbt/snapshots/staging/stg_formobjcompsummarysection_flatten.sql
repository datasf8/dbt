{% snapshot stg_formobjcompsummarysection_flatten %}
    {{

        config(
          unique_key="FORMDATAID||'-'||FORMCONTENTID",
          strategy='check',
          check_cols='all',
        )

    }}
select
Trim(VALUE:"formContentId"::STRING) AS formContentId,
Trim(VALUE:"formDataId"::STRING) AS formDataId,
Trim(VALUE:"overallCompRating":"comment"::STRING) AS overallCompRating_comment,
Trim(VALUE:"overallCompRating":"commentKey"::STRING) AS overallCompRating_commentKey,
Trim(VALUE:"overallCompRating":"commentLabel"::STRING) AS overallCompRating_commentLabel,
Trim(VALUE:"overallCompRating":"commentPermission"::STRING) AS overallCompRating_commentPermission,
Trim(VALUE:"overallCompRating":"firstName"::STRING) AS overallCompRating_firstName,
Trim(VALUE:"overallCompRating":"formContentId"::STRING) AS overallCompRating_formContentId,
Trim(VALUE:"overallCompRating":"formDataId"::STRING) AS overallCompRating_formDataId,
Trim(VALUE:"overallCompRating":"fullName"::STRING) AS overallCompRating_fullName,
Trim(VALUE:"overallCompRating":"itemId"::STRING) AS overallCompRating_itemId,
Trim(VALUE:"overallCompRating":"lastName"::STRING) AS overallCompRating_lastName,
Trim(VALUE:"overallCompRating":"rating"::STRING) AS overallCompRating_rating,
Trim(VALUE:"overallCompRating":"ratingKey"::STRING) AS overallCompRating_ratingKey,
Trim(VALUE:"overallCompRating":"ratingLabel"::STRING) AS overallCompRating_ratingLabel,
Trim(VALUE:"overallCompRating":"ratingPermission"::STRING) AS overallCompRating_ratingPermission,
Trim(VALUE:"overallCompRating":"ratingType"::STRING) AS overallCompRating_ratingType,
Trim(VALUE:"overallCompRating":"sectionIndex"::STRING) AS overallCompRating_sectionIndex,
Trim(VALUE:"overallCompRating":"textRating"::STRING) AS overallCompRating_textRating,
Trim(VALUE:"overallCompRating":"userId"::STRING) AS overallCompRating_userId,
Trim(VALUE:"overallObjRating":"comment"::STRING) AS overallObjRating_comment,
Trim(VALUE:"overallObjRating":"commentKey"::STRING) AS overallObjRating_commentKey,
Trim(VALUE:"overallObjRating":"commentLabel"::STRING) AS overallObjRating_commentLabel,
Trim(VALUE:"overallObjRating":"commentPermission"::STRING) AS overallObjRating_commentPermission,
Trim(VALUE:"overallObjRating":"firstName"::STRING) AS overallObjRating_firstName,
Trim(VALUE:"overallObjRating":"formContentId"::STRING) AS overallObjRating_formContentId,
Trim(VALUE:"overallObjRating":"formDataId"::STRING) AS overallObjRating_formDataId,
Trim(VALUE:"overallObjRating":"fullName"::STRING) AS overallObjRating_fullName,
Trim(VALUE:"overallObjRating":"itemId"::STRING) AS overallObjRating_itemId,
Trim(VALUE:"overallObjRating":"lastName"::STRING) AS overallObjRating_lastName,
Trim(VALUE:"overallObjRating":"rating"::STRING) AS overallObjRating_rating,
Trim(VALUE:"overallObjRating":"ratingKey"::STRING) AS overallObjRating_ratingKey,
Trim(VALUE:"overallObjRating":"ratingLabel"::STRING) AS overallObjRating_ratingLabel,
Trim(VALUE:"overallObjRating":"ratingPermission"::STRING) AS overallObjRating_ratingPermission,
Trim(VALUE:"overallObjRating":"ratingType"::STRING) AS overallObjRating_ratingType,
Trim(VALUE:"overallObjRating":"sectionIndex"::STRING) AS overallObjRating_sectionIndex,
Trim(VALUE:"overallObjRating":"textRating"::STRING) AS overallObjRating_textRating,
Trim(VALUE:"overallObjRating":"userId"::STRING) AS overallObjRating_userId,
Trim(VALUE:"sectionCommentsLabel"::STRING) AS sectionCommentsLabel,
Trim(VALUE:"sectionDescription"::STRING) AS sectionDescription,
Trim(VALUE:"sectionIndex"::STRING) AS sectionIndex,
Trim(VALUE:"sectionName"::STRING) AS sectionName
from {{ ref('stg_formobjcompsummarysection') }}
,lateral flatten ( input => src)
  where dbt_valid_to is null
qualify
            row_number() over (
                partition by FORMDATAID, FORMCONTENTID
                order by DBT_UPDATED_AT desc
            )
            = 1
{% endsnapshot %}
