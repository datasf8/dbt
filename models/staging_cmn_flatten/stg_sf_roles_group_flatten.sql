{{ config(materialized='table',unique_key='SFROLEID',transient=false) }}

select distinct
split_part(split_part(file_name,'/',-1),'_',1) as SFROLEID,
collate(trim (f2.value:groupID),'en-ci') AS GRTGroup,
collate(trim (f2.value:groupName),'en-ci') AS GRTGroupName,
collate(trim (f3.value:groupID),'en-ci') AS TGTGroup,
collate(trim (f3.value:groupName),'en-ci') AS TGTGroupName
from {{ ref('stg_sf_roles_group') }}
,lateral flatten ( input => src:d:results, OUTER => TRUE) fl
,lateral flatten ( input => fl.value:accessGroups:results) f2
,lateral flatten ( input => fl.value:targetGroups:results) f3
WHERE fl.value:status = '1'