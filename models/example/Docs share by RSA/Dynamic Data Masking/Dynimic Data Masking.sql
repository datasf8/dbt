-- This scripts creates a function that applies to a masking policy based on current role.

-- Set session context
;
USE ROLE sysadmin;
CREATE OR REPLACE DATABASE LOREAL;
USE DATABASE LOREAL;
CREATE OR REPLACE SCHEMA ddm_demo;
USE SCHEMA ddm_demo;
USE WAREHOUSE ARP_TEACHER_XS_WH;

-- Creating a fuction could be useful to centrailze the Role Masking Policy Management and decoupling the access policy with the masking policy
-- Code snippet that returns 'allow' or 'do not allow' based on a ROLE.
-- The subquery can be replaced to select this list from an table of allowed ip addresses or ranges

DROP FUNCTION function_allow_access();
DROP FUNCTION function_allow_access_user();

CREATE OR REPLACE FUNCTION function_allow_access()
RETURNS varchar
as
$$
  select iff(current_role() = 'TEST_MASKING_ROLE', 'allow', 'do not allow')
$$;



CREATE OR REPLACE FUNCTION function_allow_access_user()
RETURNS varchar
as
$$
  select iff(current_user() = 'ARAJOLA', 'allow', 'do not allow')
$$;


CREATE OR REPLACE masking policy creditcard_mask_user AS (val string) RETURNS string ->
  CASE
    WHEN function_allow_access() = 'allow' then cast(val as string)
    ELSE cast('*******' as string)
  END;

SHOW FUNCTIONS LIKE '%allow_access%';

-- Test function. Should return 'do not allow' for role access
SELECT function_allow_access();
SELECT function_allow_access_user();

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

USE ROLE SYSADMIN;
-- Test function. Will return 'do not allow', even if the role that has access rolles up to SysAdmin
-- this allows for more control over the unmasked dataset
SELECT function_allow_access();

USE ROLE SYSADMIN;
USE DATABASE LOREAL;
USE SCHEMA DDM_DEMO;
GRANT USAGE ON FUNCTION function_allow_access() to ROLE TEST_MASKING_ROLE;
GRANT USAGE ON FUNCTION function_allow_access_user() to ROLE TEST_MASKING_ROLE;

USE ROLE TEST_MASKING_ROLE;
USE DATABASE LOREAL;
USE SCHEMA DDM_DEMO;

-- This should resolve to true after switching to the test_masking_role
SELECT function_allow_access();

USE ROLE SYSADMIN;

-- create a masking policy on email address based on value returned by fn_allow_access() function.
-- Additional masking policies can be created on required fields based on same conditions.

CREATE OR REPLACE masking policy creditcard_mask AS (val int, val2 string) RETURNS int ->
  CASE
    WHEN EXISTS (select 1 from masking_driver
    where role=current_role()
    and ct = val2)


    then val
    ELSE cast('00000000000'||RIGHT(val,4) as int)
  END;




  CREATE OR REPLACE masking policy SSN_mask AS (val varchar) RETURNS varchar ->
  CASE
    WHEN function_allow_access() = 'allow' then val
    ELSE cast('###-###-'||RIGHT(val,4) as varchar)
  END;


-- Create test table with email address and insert few rows


CREATE OR REPLACE TABLE test_cc_masking  (name varchar(20), credit_card_number int, ct string);
INSERT INTO test_cc_masking values
  ('andrea','2222222222222222', 'EUROPE'),
  ('david','1111111111111111', 'USA');

  CREATE OR REPLACE TABLE test_SSN_masking  (name varchar(20), SSN varchar);
INSERT INTO test_SSN_masking values
  ('andrea','123-123-1233'),
  ('david','987-987-9876');

select * from test_cc_masking;
select * from test_SSN_masking;
---Commands for Datamasking
SHOW MASKING POLICIES;
DESC MASKING POLICY CREDITCARD_MASK;

select *
  from table(information_schema.policy_references(policy_name => 'LOREAL.DDM_DEMO.creditcard_mask'));


-- Will return all rows with clear CC Numbers as masking policy has not been applied
USE ROLE sysadmin;
select * from test_cc_masking;
select * from test_SSN_masking;

-- set masking policy based on fn_allow_access()
ALTER TABLE test_cc_masking MODIFY COLUMN credit_card_number SET masking policy  creditcard_mask;



SELECT * FROM test_cc_masking; -- should return all rows with masked CC Numbers;
select * from test_SSN_masking;


USE ROLE TEST_MASKING_ROLE;
SELECT * FROM LOREAL.DDM_DEMO.test_cc_masking; -- should return all rows clear masked CC numbers;

-- remove masking policy from test table
USE ROLE SYSADMIN;
ALTER TABLE test_cc_masking_clone MODIFY COLUMN credit_card_number UNSET masking policy ;
ALTER TABLE test_SSN_masking MODIFY COLUMN SSN UNSET masking policy ;


--- lets test for clones created by Sysadmin
create table test_cc_masking_clone clone test_cc_masking;

USE ROLE SYSADMIN;
select ct from (SELECT * FROM test_cc_masking_clone); -- should return all rows with masked CC Numbers;

USE ROLE TEST_MASKING_ROLE;
SELECT * FROM test_cc_masking_clone; -- should return all rows clear masked CC numbers;
