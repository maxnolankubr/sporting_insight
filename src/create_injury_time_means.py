from snowflake.snowpark import Session
import os
import snowflake.snowpark.functions as F


def get_mean_injury_time(session):
    '''
    Reads data from INJURY_TIME_TABLE_STAT table and calculates the means for injury time columns using Snowpark. 
    Means are stored in a new dataframe which is then saved to a View in Snowflake.
    '''
    
    
    session.use_schema('HARMONIZED')
    injury_time = session.table("FOOTBALL_DATA.INJURY_TIME_TABLE_STAT").select(F.col("TIMESTAMP"),
                                                                    F.col("SEQUENCE"),
                                                                    F.col("PUBLICCEID"),
                                                                    F.col("HOMETEAM"),
                                                                    F.col("AWAYTEAM"),
                                                                    F.col("FIRSTHALFEXPECTEDINJURYTIME"),
                                                                    F.col("SECONDHALFEXPECTEDINJURYTIME"),
                                                                    F.col("FIRSTHALFUPDATEDINJURYTIME"),
                                                                    F.col("SECONDHALFUPDATEDINJURYTIME"),
                                                                    F.col("CLOCKMINUTE"),
                                                                    F.col("CLOCKSECOND"))
    
    injurytime_cols = ['FIRSTHALFEXPECTEDINJURYTIME', 'SECONDHALFEXPECTEDINJURYTIME', 
                   'FIRSTHALFUPDATEDINJURYTIME', 'SECONDHALFUPDATEDINJURYTIME']

    # Calculate averages directly from the DataFrame
    averages = [injury_time.selectExpr(f"avg({col})").collect()[0][0] for col in injurytime_cols]

    # Create a list of tuples containing metric names and their corresponding average values
    avg_data = [(col, avg) for col, avg in zip(injurytime_cols, averages)]

    # Create a new DataFrame for the averages with metrics as rows and column as 'metric' and 'value'
    avg_df = session.create_dataframe(avg_data, schema=["metric", "mean"])
    
    avg_df.create_or_replace_view("MEAN_INJURY_TIME_VIEW")

# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    get_mean_injury_time(session)

    session.close()