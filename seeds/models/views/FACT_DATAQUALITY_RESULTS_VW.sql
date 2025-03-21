SELECT hash(concat(F.RULE_ID,F.COLUMN_NAME )) as ORDER_ID ,
(F.RULE_ID*1000 + G.ordinal_position) AS Sorting_ID ,
F.*
From {{ ref("FACT_DATAQUALITY_RESULTS") }} F

LEFT OUTER JOIN  {{ ref("DIM_PARAM_DATAQUALITY_QUERIES_FIELDS_ORDER") }} G
on (hash(concat(F.RULE_ID,F.COLUMN_NAME ))=hash(concat(G.RULE_ID,G.COLUMN_NAME )))

ORDER BY (F.RULE_ID*1000 + G.ordinal_position)