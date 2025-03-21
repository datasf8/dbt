{% snapshot stg_formcustomsection_flatten %}
    {{

        config(
          unique_key= "FORMDATAID||'-'||FORMCONTENTID||'-'||SECTIONINDEX||'-'||OTHERSRATINGCOMMENT_USERID||'-'||CUSTOMELEMENT_ELEMENTKEY",
          strategy='check',
          check_cols='all',
          
        )

    }}
select * from (
select
Trim(SRC:"d":"formContentId"::STRING) AS formContentId,
Trim(SRC:"d":"formDataId"::STRING) AS formDataId,
Trim(SRC:"d":"sectionDescription"::STRING) AS sectionDescription,
Trim(SRC:"d":othersRatingComment:results[0]:"comment"::STRING) AS othersRatingComment_comment,
Trim(SRC:"d":othersRatingComment:results[0]:"commentKey"::STRING) AS othersRatingComment_commentKey,
Trim(SRC:"d":othersRatingComment:results[0]:"commentLabel"::STRING) AS othersRatingComment_commentLabel,
Trim(SRC:"d":othersRatingComment:results[0]:"commentPermission"::STRING) AS othersRatingComment_commentPermission,
Trim(SRC:"d":othersRatingComment:results[0]:"firstName"::STRING) AS othersRatingComment_firstName,
Trim(SRC:"d":othersRatingComment:results[0]:"formContentId"::STRING) AS othersRatingComment_formContentId,
Trim(SRC:"d":othersRatingComment:results[0]:"formDataId"::STRING) AS othersRatingComment_formDataId,
Trim(SRC:"d":othersRatingComment:results[0]:"fullName"::STRING) AS othersRatingComment_fullName,
Trim(SRC:"d":othersRatingComment:results[0]:"itemId"::STRING) AS othersRatingComment_itemId,
Trim(SRC:"d":othersRatingComment:results[0]:"lastName"::STRING) AS othersRatingComment_lastName,
Trim(SRC:"d":othersRatingComment:results[0]:"rating"::STRING) AS othersRatingComment_rating,
Trim(SRC:"d":othersRatingComment:results[0]:"ratingKey"::STRING) AS othersRatingComment_ratingKey,
Trim(SRC:"d":othersRatingComment:results[0]:"ratingLabel"::STRING) AS othersRatingComment_ratingLabel,
Trim(SRC:"d":othersRatingComment:results[0]:"ratingPermission"::STRING) AS othersRatingComment_ratingPermission,
Trim(SRC:"d":othersRatingComment:results[0]:"ratingType"::STRING) AS othersRatingComment_ratingType,
Trim(SRC:"d":othersRatingComment:results[0]:"sectionIndex"::STRING) AS othersRatingComment_sectionIndex,
Trim(SRC:"d":othersRatingComment:results[0]:"textRating"::STRING) AS othersRatingComment_textRating,
NVL(Trim(SRC:"d":othersRatingComment:results[0]:"userId"::STRING),'NULLL') AS othersRatingComment_userId,
Trim(SRC:"d":"sectionIndex"::STRING) AS sectionIndex,
Trim(SRC:"d":"sectionName"::STRING) AS sectionName,
Trim(src:d:customElement:results[0]:"checked"::STRING) AS customElement_checked,
Trim(src:d:customElement:results[0]:"dateFormat") AS customElement_dateFormat,
Trim(src:d:customElement:results[0]:"editable"::STRING) AS customElement_editable,
Trim(src:d:customElement:results[0]:"elementIndex"::STRING) AS customElement_elementIndex,
NVL(Trim(src:d:customElement:results[0]:"elementKey"::STRING),'NULLL') AS customElement_elementKey,
Trim(src:d:customElement:results[0]:"formContentId"::STRING) AS customElement_formContentId,
Trim(src:d:customElement:results[0]:"formDataId"::STRING) AS customElement_formDataId,
Trim(src:d:customElement:results[0]:"itemId"::STRING) AS customElement_itemId,
Trim(src:d:customElement:results[0]:"maximumValue"::STRING) AS customElement_maximumValue,
Trim(src:d:customElement:results[0]:"minimumValue"::STRING) AS customElement_minimumValue,
Trim(src:d:customElement:results[0]:"name"::STRING) AS customElement_name,
Trim(src:d:customElement:results[0]:"required"::STRING) AS customElement_required,
Trim(src:d:customElement:results[0]:"sectionIndex"::STRING) AS customElement_sectionIndex,
Trim(src:d:customElement:results[0]:"textMaximumLength"::STRING) AS customElement_textMaximumLength,
Trim(src:d:customElement:results[0]:"textSize"::STRING) AS customElement_textSize,
Trim(src:d:customElement:results[0]:"type"::STRING) AS customElement_type,
Trim(src:d:customElement:results[0]:"value"::STRING) AS customElement_value,
Trim(src:d:customElement:results[0]:"valueKey"::STRING) AS customElement_valueKey,
Trim(src:d:customElement:results[0]:"writingAssistant"::STRING) AS customElement_writingAssistant,
DBT_UPDATED_AT as stg_dbt_updated_at
from {{ ref('stg_formcustomsection') }}
--,lateral flatten ( input => src:d:customElement:results, OUTER => TRUE) f
--,lateral flatten (input => SRC:"d":othersRatingComment:results , OUTER => TRUE) f1
 where dbt_valid_to is null
 union
select
Trim(SRC:"d":"formContentId"::STRING) AS formContentId,
Trim(SRC:"d":"formDataId"::STRING) AS formDataId,
Trim(SRC:"d":"sectionDescription"::STRING) AS sectionDescription,
Trim(SRC:"d":othersRatingComment:results[1]:"comment"::STRING) AS othersRatingComment_comment,
Trim(SRC:"d":othersRatingComment:results[1]:"commentKey"::STRING) AS othersRatingComment_commentKey,
Trim(SRC:"d":othersRatingComment:results[1]:"commentLabel"::STRING) AS othersRatingComment_commentLabel,
Trim(SRC:"d":othersRatingComment:results[1]:"commentPermission"::STRING) AS othersRatingComment_commentPermission,
Trim(SRC:"d":othersRatingComment:results[1]:"firstName"::STRING) AS othersRatingComment_firstName,
Trim(SRC:"d":othersRatingComment:results[1]:"formContentId"::STRING) AS othersRatingComment_formContentId,
Trim(SRC:"d":othersRatingComment:results[1]:"formDataId"::STRING) AS othersRatingComment_formDataId,
Trim(SRC:"d":othersRatingComment:results[1]:"fullName"::STRING) AS othersRatingComment_fullName,
Trim(SRC:"d":othersRatingComment:results[1]:"itemId"::STRING) AS othersRatingComment_itemId,
Trim(SRC:"d":othersRatingComment:results[1]:"lastName"::STRING) AS othersRatingComment_lastName,
Trim(SRC:"d":othersRatingComment:results[1]:"rating"::STRING) AS othersRatingComment_rating,
Trim(SRC:"d":othersRatingComment:results[1]:"ratingKey"::STRING) AS othersRatingComment_ratingKey,
Trim(SRC:"d":othersRatingComment:results[1]:"ratingLabel"::STRING) AS othersRatingComment_ratingLabel,
Trim(SRC:"d":othersRatingComment:results[1]:"ratingPermission"::STRING) AS othersRatingComment_ratingPermission,
Trim(SRC:"d":othersRatingComment:results[1]:"ratingType"::STRING) AS othersRatingComment_ratingType,
Trim(SRC:"d":othersRatingComment:results[1]:"sectionIndex"::STRING) AS othersRatingComment_sectionIndex,
Trim(SRC:"d":othersRatingComment:results[1]:"textRating"::STRING) AS othersRatingComment_textRating,
NVL(Trim(SRC:"d":othersRatingComment:results[1]:"userId"::STRING),'NULLL') AS othersRatingComment_userId,
Trim(SRC:"d":"sectionIndex"::STRING) AS sectionIndex,
Trim(SRC:"d":"sectionName"::STRING) AS sectionName,
Trim(src:d:customElement:results[1]:"checked"::STRING) AS customElement_checked,
Trim(src:d:customElement:results[1]:"dateFormat") AS customElement_dateFormat,
Trim(src:d:customElement:results[1]:"editable"::STRING) AS customElement_editable,
Trim(src:d:customElement:results[1]:"elementIndex"::STRING) AS customElement_elementIndex,
NVL(Trim(src:d:customElement:results[1]:"elementKey"::STRING),'NULLL') AS customElement_elementKey,
Trim(src:d:customElement:results[1]:"formContentId"::STRING) AS customElement_formContentId,
Trim(src:d:customElement:results[1]:"formDataId"::STRING) AS customElement_formDataId,
Trim(src:d:customElement:results[1]:"itemId"::STRING) AS customElement_itemId,
Trim(src:d:customElement:results[1]:"maximumValue"::STRING) AS customElement_maximumValue,
Trim(src:d:customElement:results[1]:"minimumValue"::STRING) AS customElement_minimumValue,
Trim(src:d:customElement:results[1]:"name"::STRING) AS customElement_name,
Trim(src:d:customElement:results[1]:"required"::STRING) AS customElement_required,
Trim(src:d:customElement:results[1]:"sectionIndex"::STRING) AS customElement_sectionIndex,
Trim(src:d:customElement:results[1]:"textMaximumLength"::STRING) AS customElement_textMaximumLength,
Trim(src:d:customElement:results[1]:"textSize"::STRING) AS customElement_textSize,
Trim(src:d:customElement:results[1]:"type"::STRING) AS customElement_type,
Trim(src:d:customElement:results[1]:"value"::STRING) AS customElement_value,
Trim(src:d:customElement:results[1]:"valueKey"::STRING) AS customElement_valueKey,
Trim(src:d:customElement:results[1]:"writingAssistant"::STRING) AS customElement_writingAssistant,
DBT_UPDATED_AT as stg_dbt_updated_at
from {{ ref('stg_formcustomsection') }}
 where dbt_valid_to is null
 )
qualify
            row_number() over (
                partition by FORMDATAID, FORMCONTENTID,SECTIONINDEX, othersRatingComment_userId, customElement_elementKey
                order by stg_dbt_updated_at desc
            )
            = 1
{% endsnapshot %}
