select hash(concat(RULE_ID,COLUMN_NAME )) ORDER_ID,* 
from {{ ref("DIM_PARAM_DATAQUALITY_QUERIES_FIELDS_ORDER") }}
where  STATUS='A'