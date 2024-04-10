USE ROLE SI_ROLE;
USE WAREHOUSE SI_WH;
USE SCHEMA DB_MODELLING.HARMONIZED;


-- ----------------------------------------------------------------------------
-- Step #1: Create the tasks to call our Python stored procedures
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK MEAN_INJURY_TIME_TASK
WAREHOUSE = SI_WH
SCHEDULE = '60 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('MEAN_INJURY_TIME_STREAM')
AS
CALL HARMONIZED.MEAN_INJURY_TIME_SP();

-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

EXECUTE TASK MEAN_INJURY_TIME_TASK;

