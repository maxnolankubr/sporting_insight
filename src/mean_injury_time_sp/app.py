from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    '''
    Checks if table already exists.
    '''
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    print('yes')
    return exists

def create_mean_injury_time(session):
    '''
    Creates MEAN_INJURY_TIME table based on MEAN_INJURY_TIME_VIEW and adds new META_UPDATED_AT column for change tracking.
    '''
    _ = session.sql(f"CREATE TABLE HARMONIZED.MEAN_INJURY_TIME LIKE HARMONIZED.MEAN_INJURY_TIME_VIEW").collect()
    _ = session.sql(f"ALTER TABLE HARMONIZED.MEAN_INJURY_TIME ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()

def create_mean_injury_time_stream(session):
    '''
    Creating stream on table
    '''
    _ = session.sql(f"CREATE STREAM HARMONIZED.MEAN_INJURY_TIME_STREAM ON TABLE HARMONIZED.MEAN_INJURY_TIME").collect()

def merge_order_updates(session):
    '''
    Merges MEAN_INJURY_TIME_VIEW into MEAN_INJURY_TIME, matching on METRIC column
    '''

   # below code and at end of function serves to temporarily alter the size of the warehouse to process large amounts of data - use if required
   # _ = session.sql('ALTER WAREHOUSE SI_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    source = session.table(f'HARMONIZED.MEAN_INJURY_TIME_VIEW')
    target = session.table(f'HARMONIZED.MEAN_INJURY_TIME')

    cols_to_update = {c: source[c] for c in source.schema.names if "METADATA" not in c}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    target.merge(source, target['METRIC'] == source['METRIC'], \
                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

#    _ = session.sql('ALTER WAREHOUSE SI_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    '''
    Create the MEAN_INJURY_TIME table and MEAN_INJURY_TIME_STREAM stream if they don't exist and perform merge operation
    '''
    if not table_exists(session, schema='HARMONIZED', name='MEAN_INJURY_TIME'):
        create_mean_injury_time(session)
        create_mean_injury_time_stream(session)
        
    merge_order_updates(session)

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
        print(main(session, *sys.argv[1:]))  
    else:
        print(main(session))

    session.close()