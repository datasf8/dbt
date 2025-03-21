Select 
YEAR,
EMP_ID, 
EMP_SK,
RATING_CATEGORY,
Case RATING_CATEGORY When 'People Goals' Then PG_RATING 
                     When 'Business Goals' Then BG_RATING
                     When 'Progress in the job' Then OR_RATING 
                     Else 6 End As VALUE,
Case RATING_CATEGORY When 'People Goals' Then Case when PG_RATING = 5 Then '5.0 - Outstanding' Else PG_RATING_LABEL End
                     When 'Business Goals' Then Case when BG_RATING = 5 Then '5.0 - Outstanding' Else BG_RATING_LABEL End
                     When 'Progress in the job' Then Case when OR_RATING = 5 Then '5.0 - Outstanding' Else OR_RATING_LABEL End 
                     Else 'unrated' End As VALUE_LABEL
from {{ ref('ye_form_attributes_vw') }}
Inner Join (
Select 'People Goals' As RATING_CATEGORY
Union 
Select 'Business Goals'
Union 
Select 'Progress in the job'
) Param on 1 = 1
