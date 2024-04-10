from datetime import date
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def main(session):
    today = date.today()
    session.use_schema('EXTERNAL')
    source = session.table(f'HARMONIZED.MEAN_INJURY_TIME')
    _ =  source.write.copy_into_location(f"@DB_MODELLING.EXTERNAL.EXPORT_STAGE/MEAN_INJURY_TIME/{today}.csv", file_format_type = "csv", 
                    format_type_options ={'COMPRESSION': 'None', 'FIELD_DELIMITER':','}, header=True, overwrite=True, single=True )
    
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