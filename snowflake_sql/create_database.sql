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
CREATE OR REPLACE DATABASE DB_MODELLING;
GRANT OWNERSHIP ON DATABASE DB_MODELLING TO ROLE SI_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE SI_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE SI_WH TO ROLE SI_ROLE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE SI_ROLE;
USE WAREHOUSE SI_WH;
USE DATABASE DB_MODELLING;

-- Schemas
CREATE OR REPLACE SCHEMA EXTERNAL;
CREATE OR REPLACE SCHEMA FOOTBALL_DATA;
CREATE OR REPLACE TABLE injury_time_table_stat (
    TIMESTAMP VARCHAR,
    SEQUENCE INT,
    PUBLICCEID STRING,
    HOMETEAM STRING,
    AWAYTEAM STRING,
    FIRSTHALFEXPECTEDINJURYTIME INT,
    SECONDHALFEXPECTEDINJURYTIME INT,
    FIRSTHALFUPDATEDINJURYTIME INT,
    SECONDHALFUPDATEDINJURYTIME INT,
    CLOCKMINUTE INT,
    CLOCKSECOND INT
);
CREATE OR REPLACE SCHEMA HARMONIZED;
CREATE OR REPLACE SCHEMA ANALYTICS;
CREATE OR REPLACE SCHEMA LOGGING;

-- External stage objects
USE SCHEMA EXTERNAL;

CREATE OR REPLACE STAGE EXPORT_STAGE
    FILE_FORMAT = (TYPE = CSV)
;

CREATE OR REPLACE STAGE IMPORT_STAGE
    FILE_FORMAT = (TYPE = CSV)
;

PUT file:///workspaces/sporting_insight/data/sporting-football-injury-time-sample.csv @import_stage;
COPY INTO FOOTBALL_DATA.INJURY_TIME_TABLE_STAT 
FROM @IMPORT_STAGE/sporting-football-injury-time-sample.csv.gz 
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

-- Create events table
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EVENT TABLE DB_MODELLING.LOGGING.EVENTS;
ALTER ACCOUNT SET EVENT_TABLE = DB_MODELLING.LOGGING.EVENTS;