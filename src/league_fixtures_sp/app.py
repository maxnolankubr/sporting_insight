import time
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import logging
import os
import sys

log_folder_path = os.path.join('/tmp', 'league_fixtures_logs')
os.makedirs(log_folder_path, exist_ok=True)  # Create 'logs' folder if it doesn't exist
log_file_path = os.path.join(log_folder_path, 'league_fixtures_log.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def get_league_name(session, league_id: int) -> str:
    leagues = session.table("RAW.LEAGUES").select(F.col("LEAGUE_ID"), F.col("NAME"))
    return leagues.filter(F.col("LEAGUE_ID") == league_id).select("NAME").first()["NAME"].replace(" ","_").upper()

def get_all_league_ids(session) -> list:

    return [row.LEAGUE_ID for row in session.table("RAW.LEAGUES").select("LEAGUE_ID").distinct().collect()]

def create_league_fixtures_table(session, league_name):
    _ = session.sql(f"CREATE TABLE HARMONIZED.{league_name}_FIXTURES LIKE HARMONIZED.{league_name}_FIXTURES_V").collect()
    _ = session.sql(f"ALTER TABLE HARMONIZED.{league_name}_FIXTURES ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()

def create_league_fixtures_stream(session, league_name):
    _ = session.sql(f"CREATE STREAM HARMONIZED.{league_name}_FIXTURES_STREAM ON TABLE HARMONIZED.{league_name}_FIXTURES").collect()

def merge_order_updates(session, league_name):
    
    logging.info(f'Processing {league_name} merge')

   # _ = session.sql('ALTER WAREHOUSE SI_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    source = session.table(f'HARMONIZED.{league_name}_FIXTURES_V')
    target = session.table(f'HARMONIZED.{league_name}_FIXTURES')

    cols_to_update = {c: source[c] for c in source.schema.names if "METADATA" not in c}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}
    # merge into DIM_CUSTOMER
    target.merge(source, target['FIXTURE_ID'] == source['FIXTURE_ID'], \
                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

#    _ = session.sql('ALTER WAREHOUSE SI_WH SET WAREHOUSE_SIZE = XSMALL').collect()

    logging.info(f'Completed {league_name} merge')

def main(session: Session, league_name:str = 'all') -> str:
    # Create the ORDERS table and ORDERS_STREAM stream if they don't exist
    
    if league_name == 'all':
        logging.info('Processing all leagues')
        for league_id in get_all_league_ids(session):
            league_name = get_league_name(session, league_id)
            if not table_exists(session, schema='HARMONIZED', name=f'{league_name}_FIXTURES'):
                create_league_fixtures_table(session, league_name)
                logging.info(f'{league_name} table created')
                create_league_fixtures_stream(session, league_name)
                logging.info(f'{league_name} stream created')
            # Process data incrementally
            merge_order_updates(session, league_name)
        logging.info('Full run complete')
    else:
        logging.info(f'Processing {league_name}')
        if not table_exists(session, schema='HARMONIZED', name=f'{league_name}_FIXTURES'):
            create_league_fixtures_table(session, league_name)
            logging.info(f'{league_name} table created')
            create_league_fixtures_stream(session, league_name)
            logging.info(f'{league_name} stream created')
        # Process data incrementally
        merge_order_updates(session, league_name)


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
