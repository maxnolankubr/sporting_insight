import time
from datetime import date
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def get_league_name(session, league_id: int) -> str:
    leagues = session.table("RAW.LEAGUES").select(F.col("LEAGUE_ID"), F.col("NAME"))
    return leagues.filter(F.col("LEAGUE_ID") == league_id).select("NAME").first()["NAME"].replace(" ","_").upper()

def get_all_league_ids(session) -> list:

    return [row.LEAGUE_ID for row in session.table("RAW.LEAGUES").select("LEAGUE_ID").distinct().collect()]

def main(session, league_name = 'all'):
    today = date.today()
    session.use_schema('EXTERNAL')
    if league_name == 'all':
        print('copying all league tables into stage')
        for league_id in get_all_league_ids(session):
            league_name = get_league_name(session, league_id)
            source = session.table(f'HARMONIZED.{league_name}_FIXTURES')
            _ =  source.write.copy_into_location(f"@SI_DB.EXTERNAL.EXPORT_STAGE/{league_name}_FIXTURES/{today}.csv", file_format_type = "csv", 
                            format_type_options ={'COMPRESSION': 'None', 'FIELD_DELIMITER':','}, header=True, overwrite=True, single=True )
            print(f'{league_name} fixtures copied into stage')
    else:
        print(f'copying {league_name} into stage')
        source = session.table(f'HARMONIZED.{league_name}_FIXTURES')
        _ =  source.write.copy_into_location(f"@SI_DB.EXTERNAL.EXPORT_STAGE/{league_name}_FIXTURES/{today}.csv", file_format_type = "csv", 
                        format_type_options ={'COMPRESSION': 'None', 'FIELD_DELIMITER':','}, header=True, overwrite=True, single=True )
    print('copying finished')

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
