SELECT
EMP_SK,
--MGR_SK,
EMP_ID ,
MANAGER_ID,
FORM_TITLE,
IS_FORM_LAUNCHED,
FORM_STATUS,
PG_RATING,
PG_RATING_LABEL,
PG_RATING_LABEL_MASK,
BG_RATING,
BG_RATING_LABEL,
BG_RATING_LABEL_MASK,
OR_RATING,
OR_RATING_LABEL,
OR_RATING_LABEL_MASK,
NB_OVERALL_RATINGS,
PE_PG_RATING,
PE_PG_RATING_LABEL,
PE_PG_RATING_LABEL_MASK,
PE_BG_RATING,
PE_BG_RATING_LABEL,
PE_BG_RATING_LABEL_MASK,
PE_OR_RATING,
PE_OR_RATING_LABEL,
PE_OR_RATING_LABEL_MASK,
PE_MANAGER_COMMENTS,
PE_LMDATE,
YE_PG_RATING,
YE_PG_RATING_LABEL,
YE_PG_RATING_LABEL_MASK,
YE_BG_RATING,
YE_BG_RATING_LABEL,
YE_BG_RATING_LABEL_MASK,
YE_OR_RATING,
YE_OR_RATING_LABEL,
YE_OR_RATING_LABEL_MASK,
YE_CONVERSATION_DATE,
YE_LMDATE,
YE_MANAGER_LMDATE,
YE_MANAGER_COMMENTS,
YE_EMPLOYEE_LMDATE,
YE_EMPLOYEE_COMMENTS,
YE_MATRIX_MANAGER_LMDATE,
YE_MATRIX_MANAGER_COMMENTS,
SGN_EMPLOYEE_COMMENTS,
SGN_MANAGER_COMMENTS,
SGN_EMP_SIGN,
SGN_EMPLOYEE_LMDATE,
SGN_EMPLOYEE_DATE,
SGN_MANAGER_SIGN,
SGN_MANAGER_LMDATE,
SGN_MANAGER_DATE,
SGN_EMPSIGN,
SGN_MGRSIGN,
YEAR,
"Manager Rating People Goals",
"Manager Rating Business Goals",
"Manager Rating Progress in the Job Goals",
"Manager Rating comment for HR",
"Manager Year End comment",
"Employee Year End comment"
from {{ ref('ye_form_attributes') }}