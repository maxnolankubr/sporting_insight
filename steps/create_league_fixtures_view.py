from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import when, col, sum, count

def get_league_name(session, league_id: int) -> str:
    leagues = session.table("RAW.LEAGUES").select(F.col("LEAGUE_ID"), F.col("NAME"))
    return leagues.filter(F.col("LEAGUE_ID") == league_id).select("NAME").first()["NAME"].replace(" ","_").upper()

def get_all_league_ids(session) -> list:

    return [row.LEAGUE_ID for row in session.table("RAW.LEAGUES").select("LEAGUE_ID").distinct().collect()]

def create_league_fixtures_view (session, league_id: int):
    session.use_schema('HARMONIZED')
    fixtures = session.table("RAW.FIXTURES").select(F.col("FIXTURE_ID"), \
                                                                F.col("LEAGUE_ID"), \
                                                                F.col("HOME_TEAM_ID"), \
                                                                F.col("AWAY_TEAM_ID"), \
                                                                F.col("HOME_TEAM_GOALS"), \
                                                                F.col("AWAY_TEAM_GOALS"), \
                                                                F.col("FIXTURE_DATE"))
    teams = session.table("RAW.TEAMS").select(F.col("TEAM_ID"), F.col("NAME"))
    filter_fixtures = fixtures.filter((F.col("LEAGUE_ID")) == league_id)
    f_with_home_team = filter_fixtures.join(teams, filter_fixtures["HOME_TEAM_ID"] == teams["TEAM_ID"], rsuffix = '_home').select(
        F.col("FIXTURE_ID"),
        F.col("HOME_TEAM_ID"), \
        F.col("NAME").alias("HOME_TEAM"), \
        F.col("AWAY_TEAM_ID"), \
        F.col("HOME_TEAM_GOALS"), \
        F.col("AWAY_TEAM_GOALS"), \
        F.col("FIXTURE_DATE")
    )
    f_with_away_team = f_with_home_team.join(teams, f_with_home_team["AWAY_TEAM_ID"] == teams["TEAM_ID"], rsuffix = 'away').select(
        F.col("FIXTURE_ID"),
        F.col("HOME_TEAM_ID"), \
        F.col("HOME_TEAM"), \
        F.col("AWAY_TEAM_ID"), \
        F.col("NAME").alias("AWAY_TEAM"), \
        F.col("HOME_TEAM_GOALS"), \
        F.col("AWAY_TEAM_GOALS"), \
        F.col("FIXTURE_DATE")
    )

    league_name = get_league_name(session, league_id)

    f_with_away_team.create_or_replace_view(f"{league_name}_FIXTURES_V")

def create_league_fixtures_stream(session, league_id):
    session.use_schema('HARMONIZED')
    league_name = get_league_name(session, league_id)
    _ = session.sql(f"CREATE OR REPLACE STREAM {league_name}_FIXTURES_V_STREAM \
                        ON VIEW {league_name}_FIXTURES_V \
                        SHOW_INITIAL_ROWS = TRUE").collect()

# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    for league_id in get_all_league_ids(session):
        create_league_fixtures_view(session, league_id)
        create_league_fixtures_stream(session, league_id)
        #test_team_total_view(session, team_id)

    session.close()