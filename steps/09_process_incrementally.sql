USE ROLE SI_ROLE;
USE WAREHOUSE SI_WH;
USE DATABASE SI_DB;


-- ----------------------------------------------------------------------------
-- Step #1: Add new/remaining order data
-- ----------------------------------------------------------------------------

-- USE SCHEMA RAW_POS;

-- ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE;

-- COPY INTO ORDER_HEADER
-- FROM @external.frostbyte_raw_stage/pos/order_header/year=2022
-- FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
-- MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- COPY INTO ORDER_DETAIL
-- FROM @external.frostbyte_raw_stage/pos/order_detail/year=2022
-- FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
-- MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- -- See how many new records are in the stream (this may be a bit slow)
-- --SELECT COUNT(*) FROM HARMONIZED.POS_FLATTENED_V_STREAM;

-- ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL;


-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

USE SCHEMA HARMONIZED;

EXECUTE TASK PREMIER_LEAGUE_UPDATE_TASK;
EXECUTE TASK LA_LIGA_UPDATE_TASK;
EXECUTE TASK SERIE_A_BRASIL_UPDATE_TASK;


