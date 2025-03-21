-- Set session context
;
USE ROLE sysadmin;
CREATE OR REPLACE DATABASE LOREAL;
USE DATABASE LOREAL;
CREATE OR REPLACE SCHEMA ddm_demo;
USE SCHEMA ddm_demo;
USE WAREHOUSE ARP_TEACHER_XS_WH;



-- Create test masking role and set permissions to objects for demonstrating masking at work.
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE TEST_MASKING_ROLE;
GRANT USAGE ON DATABASE LOREAL TO ROLE TEST_MASKING_ROLE;
GRANT USAGE ON SCHEMA LOREAL.DDM_DEMO TO ROLE TEST_MASKING_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA LOREAL.DDM_DEMO TO ROLE TEST_MASKING_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA LOREAL.DDM_DEMO TO ROLE TEST_MASKING_ROLE;
GRANT USAGE ON WAREHOUSE ARP_TEACHER_XS_WH TO ROLE TEST_MASKING_ROLE;
GRANT ROLE TEST_MASKING_ROLE TO USER arajola;
GRANT ROLE TEST_MASKING_ROLE TO ROLE SYSADMIN;

--- Create masking with Driving table
USE ROLE SYSADMIN;

--- Create Driving Table
CREATE OR REPLACE TABLE masking_driver  (role varchar(20), ct string);
INSERT INTO masking_driver values
  ('TEST_MASKING_ROLE', 'EUROPE'),
  ('SYSADMIN', 'USA');


-- create a masking policy on email address based on value returned by fn_allow_access() function.
-- Additional masking policies can be created on required fields based on same conditions.

CREATE OR REPLACE masking policy creditcard_mask_et AS (val int, val2 string) RETURNS int ->
  CASE
    WHEN EXISTS (select 1 from masking_driver
    where role=current_role()
    and ct = val2)


    then val
    ELSE cast('00000000000'||RIGHT(val,4) as int)
  END;

  --- Create table to mask

CREATE OR REPLACE TABLE test_cc_masking  (name varchar(20), credit_card_number int, ct string);
INSERT INTO test_cc_masking values
  ('andrea','2222222222222222', 'EUROPE'),
  ('david','1111111111111111', 'USA');


-- Will return all rows with clear CC Numbers as masking policy has not been applied
USE ROLE sysadmin;
select * from test_cc_masking;

--- Apply
ALTER TABLE test_cc_masking MODIFY COLUMN credit_card_number SET masking policy  creditcard_mask_et using (credit_card_number, ct);

USE ROLE sysadmin;
select * from test_cc_masking;

USE ROLE TEST_MASKING_ROLE;
select * from test_cc_masking;

  SHOW MASKING POLICIES;
DESC MASKING POLICY CREDITCARD_MASK_ET;
  
