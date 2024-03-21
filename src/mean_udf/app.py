from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import logging
import os
import sys

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
