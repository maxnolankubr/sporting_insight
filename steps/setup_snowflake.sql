-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE SI_ROLE;
GRANT ROLE SI_ROLE TO ROLE SYSADMIN;
GRANT ROLE SI_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE SI_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE SI_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SI_ROLE;

-- Databases
CREATE OR REPLACE DATABASE SI_DB;
GRANT OWNERSHIP ON DATABASE SI_DB TO ROLE SI_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE SI_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE SI_WH TO ROLE SI_ROLE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE SI_ROLE;
USE WAREHOUSE SI_WH;
USE DATABASE SI_DB;

-- Schemas
CREATE OR REPLACE SCHEMA EXTERNAL;
CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA HARMONIZED;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- External stage objects
USE SCHEMA EXTERNAL;
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = CSV
;
CREATE OR REPLACE STAGE EXPORT_STAGE
    FILE_FORMAT = CSV_FORMAT
;
