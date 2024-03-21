from snowflake.snowpark import Session
import pandas as pd
import os
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import when, col, sum, count

def calculate_mean_values(input_df):
    """
    Calculate mean values for specific columns in the input DataFrame.

    Parameters:
    - input_df: Original DataFrame.

    Returns:
    - df_mean: DataFrame containing mean values.
    """
    
    # Ensure input_df is not modified in place
    input_df.DataFrame.toPandas()

    # Specify columns for mean calculation
    columns_for_mean = [
        'FIRSTHALFEXPECTEDINJURYTIME',
        'SECONDHALFEXPECTEDINJURYTIME',
        'FIRSTHALFUPDATEDINJURYTIME',
        'SECONDHALFUPDATEDINJURYTIME'
    ]

    # Calculate mean values
    mean_values = input_df[columns_for_mean].mean()

    # Create DataFrame of means
    df_mean = pd.DataFrame(mean_values, columns=['Values'])
    df_mean.index.name = 'Metric'

    print(df_mean)
    return df_mean


def create_injury_time_means (session):
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
                                                                    F.col("CLOCKSECOND")).collect()
    
    df_mean = calculate_mean_values(injury_time)

    return df_mean

# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    create_injury_time_means(session)

    session.close()