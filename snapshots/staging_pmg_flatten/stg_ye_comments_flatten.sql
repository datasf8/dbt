{% snapshot stg_ye_comments_flatten %}
    {{

        config(
          unique_key="FORMDATA_ID",
          strategy='check',
          check_cols='all',
          
        )

    }}
select * from(
select distinct
TRIM (src:d:customElement:results[0]:formDataId) AS FORMDATA_ID,
TRIM (src:d:customElement:results[0]:formContentId) as FORMCONTENT_ID,
IFF (TRIM (src:d:customElement:results[0]:value)<>'',TRIM (src:d:customElement:results[0]:value),NULL) AS CONVERSATION_DATE,
TRIM (src:d:othersRatingComment:results[0]:comment) AS COMMENTS0,
TRIM (src:d:othersRatingComment:results[1]:comment) AS COMMENTS1,
TRIM (src:d:othersRatingComment:results[0]:userId) AS USER_ID0,
TRIM (src:d:othersRatingComment:results[1]:userId) AS USER_ID1,
to_DATE(split_part(split_part(null,'(',2),'+',1)) as YE_LMD_EMP,
to_DATE(split_part(split_part(null,'(',2),'+',1)) as YE_LMD_MGR,
row_number() over (partition by formdata_id order by to_date(dbt_valid_from) desc,FILE_NAME desc) as RK
FROM {{ ref('stg_ye_comments') }}
where dbt_valid_to is null
) where RK=1 

{% endsnapshot %}