select distinct
	DYER_SK_DYER as KEY,
	DYER_CODE_DYER as CODE,
	DYER_LABEL_DYER as LABEL
    from {{ ref('dim_ye_form_ratings') }}
