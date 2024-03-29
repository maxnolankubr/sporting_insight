USE ROLE SI_ROLE;
USE WAREHOUSE SI_WH;
USE SCHEMA SI_DB.HARMONIZED;


-- ----------------------------------------------------------------------------
-- Step #1: Create the tasks to call our Python stored procedures
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK PREMIER_LEAGUE_UPDATE_TASK
WAREHOUSE = SI_WH
SCHEDULE = 'HOURLY'
WHEN
  SYSTEM$STREAM_HAS_DATA('PREMIER_LEAGUE_V_STREAM')
AS
CALL HARMONIZED.LEAGUE_FIXTURES_SP('PREMIER_LEAGUE');

CREATE OR REPLACE TASK LA_LIGA_UPDATE_TASK
WAREHOUSE = SI_WH
SCHEDULE = 'HOURLY'
WHEN
  SYSTEM$STREAM_HAS_DATA('LA_LIGA_V_STREAM')
AS
CALL HARMONIZED.LEAGUE_FIXTURES_SP('LA_LIGA');

CREATE OR REPLACE TASK SERIE_A_BRASIL_UPDATE_TASK
WAREHOUSE = SI_WH
SCHEDULE = 'HOURLY'
WHEN
  SYSTEM$STREAM_HAS_DATA('SERIE_A_BRASIL_V_STREAM')
AS
CALL HARMONIZED.LEAGUE_FIXTURES_SP('SERIE_A_BRASIL');

CREATE OR REPLACE TASK PREMIER_LEAGUE_EXPORT_TASK
WAREHOUSE = SI_WH
SCHEDULE = 'USING CRON 0 0 * * * GMT'
AS
CALL EXTERNAL.LEAGUE_FIXTURES_WRITE_TO_STAGE_SP('PREMIER_LEAGUE');

CREATE OR REPLACE TASK LA_LIGA_EXPORT_TASK
WAREHOUSE = SI_WH
SCHEDULE = 'USING CRON 0 0 * * * GMT'
AS
CALL EXTERNAL.LEAGUE_FIXTURES_WRITE_TO_STAGE_SP('LA_LIGA');

CREATE OR REPLACE TASK SERIE_A_BRASIL_EXPORT_TASK
WAREHOUSE = SI_WH
SCHEDULE = 'USING CRON 0 0 * * * GMT'
AS
CALL EXTERNAL.LEAGUE_FIXTURES_WRITE_TO_STAGE_SP('SERIE_A_BRASIL');

-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

EXECUTE TASK PREMIER_LEAGUE_UPDATE_TASK;
EXECUTE TASK LA_LIGA_UPDATE_TASK;
EXECUTE TASK SERIE_A_BRASIL_UPDATE_TASK;
EXECUTE TASK PREMIER_LEAGUE_EXPORT_TASK;
EXECUTE TASK LA_LIGA_EXPORT_TASK;
EXECUTE TASK SERIE_A_BRASIL_EXPORT_TASK;

