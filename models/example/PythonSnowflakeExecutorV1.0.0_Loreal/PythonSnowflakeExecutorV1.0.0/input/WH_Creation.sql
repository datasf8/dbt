USE ROLE COMM_DOMAIN_ADMIN;
--SN: Warehouse for ETL
CREATE OR REPLACE WAREHOUSE COMM_US_ETL_XS_WH 
WITH 
WAREHOUSE_SIZE='X-SMALL'
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = STANDARD
AUTO_SUSPEND = 60
;

CREATE OR REPLACE WAREHOUSE COMM_US_ETL_S_WH 
WITH 
WAREHOUSE_SIZE='SMALL'
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = STANDARD
AUTO_SUSPEND = 60
;

CREATE OR REPLACE WAREHOUSE COMM_US_ETL_M_WH 
WITH 
WAREHOUSE_SIZE='MEDIUM'
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = STANDARD
AUTO_SUSPEND = 60
;

CREATE OR REPLACE WAREHOUSE COMM_US_ETL_L_WH 
WITH 
WAREHOUSE_SIZE='LARGE'
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = STANDARD
AUTO_SUSPEND = 60
;

CREATE OR REPLACE WAREHOUSE COMM_US_ETL_XL_WH 
WITH 
WAREHOUSE_SIZE='X-LARGE'
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = STANDARD
AUTO_SUSPEND = 60
;
--EN: Warehouse for ETL

--Warehouse for users
CREATE OR REPLACE WAREHOUSE COMM_US_USER_XS_WH 
WITH 
WAREHOUSE_SIZE='X-SMALL'
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = STANDARD
AUTO_SUSPEND = 60
;


--SN:Warehouse for BI
CREATE OR REPLACE WAREHOUSE COMM_US_RPT_EXTRACT_WH 
WITH 
WAREHOUSE_SIZE='MEDIUM'
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = STANDARD
AUTO_SUSPEND = 60
;

CREATE OR REPLACE WAREHOUSE COMM_US_RPT_LIVE_WH 
WITH 
WAREHOUSE_SIZE='MEDIUM'
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = STANDARD
AUTO_SUSPEND = 300
;
--EN:Warehouse for BI