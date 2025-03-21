{% snapshot stg_formaudittrails_flatten %}
    {{

        config(
          unique_key= "AUDITTRAILID",
          strategy='check',
          check_cols='all',
          
        )

    }}

select
Trim(VALUE:"auditTrailAction"::STRING) AS auditTrailAction,
Trim(VALUE:"auditTrailCoSender"::STRING) AS auditTrailCoSender,
Trim(VALUE:"auditTrailComment"::STRING) AS auditTrailComment,
Trim(VALUE:"auditTrailId"::STRING) AS auditTrailId,
DATEADD(MS, replace(split_part(split_part(value:"auditTrailLastModified", '(', 2), '+', 1), ')/', ''), '1970-01-01') AS auditTrailLastModified,
Trim(VALUE:"auditTrailRecipient"::STRING) AS auditTrailRecipient,
Trim(VALUE:"auditTrailSendProxy"::STRING) AS auditTrailSendProxy,
Trim(VALUE:"auditTrailSender"::STRING) AS auditTrailSender,
Trim(VALUE:"formContentAssociatedStepId"::STRING) AS formContentAssociatedStepId,
Trim(VALUE:"formContentId"::STRING) AS formContentId,
Trim(VALUE:"formDataId"::STRING) AS formDataId
from {{ ref('stg_formaudittrails') }}
,lateral flatten ( input => src:d:results, OUTER => TRUE) where dbt_valid_to is null 

qualify
            row_number() over (
                partition by formDataId, formContentAssociatedStepId
                order by auditTrailLastModified desc, auditTrailId desc
            )
            = 1
{% endsnapshot %}
