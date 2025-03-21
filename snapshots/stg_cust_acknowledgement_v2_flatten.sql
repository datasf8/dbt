{% snapshot stg_cust_acknowledgement_v2_flatten %}
    {{
        config(
            unique_key="externalCode",
            strategy="check",
            check_cols="all",
            target_schema="SDDS_STG_SCH",
        )
    }}

    select
        trim(value:"externalCode"::string) as externalcode,
        trim(
            value:"cust_AcknowledgementList_externalCode"::string
        ) as cust_acknowledgementlist_externalcode,
        trim(value:"cust_model_id"::string) as cust_model_id,
        dateadd(
            ms,
            replace(
                split_part(split_part(value:"cust_validationDate", '(', 2), '+', 1),
                ')/',
                ''
            ),
            '1970-01-01'
        ) as cust_validationdate,
        dateadd(
            ms,
            replace(
                split_part(split_part(value:"lastModifiedDateTime", '(', 2), '+', 1),
                ')/',
                ''
            ),
            '1970-01-01'
        ) as lastmodifieddatetime,
        dateadd(
            ms,
            replace(
                split_part(split_part(value:"cust_dateTime", '(', 2), '+', 1), ')/', ''
            ),
            '1970-01-01'
        ) as cust_datetime,
        trim(value:"lastModifiedBy"::string) as lastmodifiedby,
        dateadd(
            ms,
            replace(
                split_part(split_part(value:"createdDateTime", '(', 2), '+', 1),
                ')/',
                ''
            ),
            '1970-01-01'
        ) as createddatetime,
        trim(value:"mdfSystemRecordStatus"::string) as mdfsystemrecordstatus,
        trim(value:"cust_paperSignature"::string) as cust_papersignature,
        dateadd(
            ms,
            replace(
                split_part(split_part(value:"cust_consultationDate", '(', 2), '+', 1),
                ')/',
                ''
            ),
            '1970-01-01'
        ) as cust_consultationdate,
        trim(value:"createdBy"::string) as createdby,
        trim(value:"externalName"::string) as externalname,
        trim(value:"cust_status"::string) as cust_status,
        trim(value:"cust_comment"::string) as cust_comment,
        trim(value:"cust_seqnr"::string) as cust_seqnr
    from
        {{ ref("stg_cust_acknowledgement_v2") }},
        lateral flatten(input => src:d:results, outer => true)
    where dbt_valid_to is null
    qualify
        row_number() over (partition by externalcode order by lastmodifieddatetime desc)
        = 1
{% endsnapshot %}
