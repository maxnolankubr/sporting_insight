from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import when, col, sum, count


def get_team_name(session, team_id: int) -> str:
    teams = session.table("RAW.TEAMS").select(F.col("TEAM_ID"), F.col("NAME"))
    
    return teams.filter(F.col("TEAM_ID")== team_id).select("NAME").first()["NAME"].replace(" ", "_")

def get_all_team_ids(session) -> list:
    
    return [row.TEAM_ID for row in session.table("RAW.TEAMS").select("TEAM_ID").distinct().collect()]
    

def create_team_totals_view(session, team_id: int):
    session.use_schema('HARMONIZED')
    fixtures = session.table("RAW.FIXTURES").select(F.col("FIXTURE_ID"), \
                                                                F.col("LEAGUE_ID"), \
                                                                F.col("HOME_TEAM_ID"), \
                                                                F.col("AWAY_TEAM_ID"), \
                                                                F.col("HOME_TEAM_GOALS"), \
                                                                F.col("AWAY_TEAM_GOALS"), \
                                                                F.col("FIXTURE_DATE"))
    leagues = session.table("RAW.LEAGUES").select(F.col("LEAGUE_ID"), F.col("NAME"))
    fixtures_filtered = fixtures.filter(((F.col("HOME_TEAM_ID"))== team_id) | (F.col("AWAY_TEAM_ID")== team_id))
    goals_for = when(F.col("HOME_TEAM_ID")==team_id, col("HOME_TEAM_GOALS")).otherwise(col("AWAY_TEAM_GOALS"))
    goals_against = when(col("AWAY_TEAM_ID") == team_id, col("HOME_TEAM_GOALS")).otherwise(col("AWAY_TEAM_GOALS"))
    fixtures_for_against = fixtures_filtered.withColumn("GOALS_FOR", goals_for).withColumn("GOALS_AGAINST", goals_against)
    points_df = fixtures_for_against.withColumn(
                        "POINTS",
                        when(col("GOALS_FOR") > col("GOALS_AGAINST"), 3)
                        .when(col("GOALS_FOR") == col("GOALS_AGAINST"), 1)
                        .otherwise(0)
    )
    totals_df = points_df.groupBy("LEAGUE_ID").agg(
                        sum("GOALS_FOR").alias("GOALS_FOR"),
                        sum("GOALS_AGAINST").alias("GOALS_AGAINST"),
                        sum("POINTS").alias("POINTS"),
                        count("FIXTURE_ID").alias("GAMES_PLAYED")
                    )
    totals_with_leagues = totals_df.join(leagues, totals_df["LEAGUE_ID"] == leagues["LEAGUE_ID"], rsuffix ='_leagues')
    final_df = totals_with_leagues.select(
        F.col("NAME").alias("LEAGUE_NAME"), \
        F.col("GOALS_FOR"), \
        F.col("GOALS_AGAINST"), \
        F.col("POINTS"), \
        F.col("GAMES_PLAYED")
    )

    team_name = get_team_name(session, team_id)

    final_df.create_or_replace_view(f"{team_name}_TOTAL_V")
   

def create_team_total_dynamic_table(session, team_id):
    session.use_schema('HARMONIZED')
    team_name = get_team_name(session, team_id)
    _ = session.sql(f"CREATE OR REPLACE DYNAMIC TABLE {team_name} \
                        TARGET_LAG = '60 minutes' \
                        WAREHOUSE = 'SI_WH' \
                        AS \
                        SELECT * FROM {team_name}_TOTAL_V").collect()

# def test_pos_view(session, team_id):
#     session.use_schema('HARMONIZED')
#     team_name = get_team_name(session, team_id)
#     tv = session.table(f'{team_name}_TOTAL_V')
#     tv.limit(5).show()


# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    for team_id in get_all_team_ids(session):
        create_team_totals_view(session, team_id)
        create_team_total_dynamic_table(session, team_id)
        #test_team_total_view(session, team_id)

    session.close()
