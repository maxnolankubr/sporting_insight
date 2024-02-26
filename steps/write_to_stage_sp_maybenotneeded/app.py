import time
from datetime import date
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def write_to_stage(session, schema, stream, file_format):
    today = date.today()

    _ = session.sql(f"COPY INTO '@EXPORT_STAGE/{stream}/{today}' FROM (SELECT * FROM {schema}.{stream} \
                    FILE_FORMAT = {file_format} \
                    OVERWRITE = TRUE \
                    DETAILED OUTPUT = TRUE").collect
    

def main(session: Session) -> str:
    # Create the DAILY_CITY_METRICS table if it doesn't exist
    if not table_exists(session, schema='ANALYTICS', name='DAILY_CITY_METRICS'):
        create_daily_city_metrics_table(session)
    
    merge_daily_city_metrics(session)
#    session.table('ANALYTICS.DAILY_CITY_METRICS').limit(5).show()

    return f"Successfully processed DAILY_CITY_METRICS"


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
