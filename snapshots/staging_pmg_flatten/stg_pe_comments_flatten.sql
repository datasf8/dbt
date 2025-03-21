{% snapshot stg_pe_comments_flatten %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}

select distinct
TRIM (VALUE:"formDataId") AS FORMDATA_ID,
TRIM (VALUE:"formContentId") as FORMCONTENT_ID,
TRIM (VALUE:"comment") as COMMENTS
FROM
{{ ref('stg_pe_comments') }}
,lateral flatten ( input => src:d:othersRatingComment:results, OUTER => TRUE)

{% endsnapshot %}