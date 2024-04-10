# Import python packages
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from scipy.stats import ttest_ind
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Injury Time Analysis")

# Get the current credentials
session = get_active_session()

#  Create an example dataframe
#  Note: this is just some dummy data, but you can easily connect to your Snowflake data
#  It is also possible to query data using raw SQL using session.sql() e.g. session.sql("select * from table")
created_dataframe = session.sql("""
    select
        FIRSTHALFEXPECTEDINJURYTIME as "EXPECTED_1ST_IT", 
        SECONDHALFEXPECTEDINJURYTIME as "EXPECTED_2ND_IT", 
        FIRSTHALFUPDATEDINJURYTIME as "UPDATED_1ST_IT", 
        SECONDHALFUPDATEDINJURYTIME as "UPDATED_2ND_IT"
    from 
        DB_MODELLING.FOOTBALL_DATA.INJURY_TIME_TABLE_STAT
"""
)

# Execute the query and convert it into a Pandas dataframe
queried_data = created_dataframe.to_pandas()

# Create a simple bar chart
# See docs.streamlit.io for more types of charts
st.subheader("Overall Expected/Uptated 1st/2nd half Injury Time repartition")
# Create subplots
fig, axes = plt.subplots(2, 2, figsize=(10, 8))

# Plot boxplots
sns.boxplot(data=queried_data['EXPECTED_1ST_IT'], ax=axes[0, 0])
axes[0, 0].set_title('Boxplot for EXPECTED_1ST_IT')

sns.boxplot(data=queried_data['EXPECTED_2ND_IT'], ax=axes[0, 1])
axes[0, 1].set_title('Boxplot for EXPECTED_2ND_IT')

sns.boxplot(data=queried_data['UPDATED_1ST_IT'], ax=axes[1, 0])
axes[1, 0].set_title('Boxplot for UPDATED_1ST_IT')

sns.boxplot(data=queried_data['UPDATED_2ND_IT'], ax=axes[1, 1])
axes[1, 1].set_title('Boxplot for UPDATED_2ND_IT')

# Add overall title and adjust layout
plt.suptitle('Boxplots Expected/Updated for the 1st/2nd half for all competitions')
plt.tight_layout()

# Display the plot in Streamlit
st.pyplot(fig)

st.subheader("Underlying data")
st.dataframe(queried_data, use_container_width=True)

# Perform t-test for 1st half
t_statistic_1st_overall, p_value_1st_overall = ttest_ind(queried_data['EXPECTED_1ST_IT'], queried_data['UPDATED_1ST_IT'])

# Perform t-test for 2nd half
t_statistic_2nd_overall, p_value_2nd_overall = ttest_ind(queried_data['EXPECTED_2ND_IT'], queried_data['UPDATED_2ND_IT'])

# Setting up Streamlit app title and header
st.title('Injury Time Analysis')
st.header('T-test Results')

# Displaying results with proper formatting and styling
st.write('## 1st Half Injury Time Analysis')
st.write(f"**t-statistic for Expected 1st IT vs Updated 1st IT Overall:** {t_statistic_1st_overall:.2f}")
st.write(f"**p-value:** {p_value_1st_overall:.4f}")

st.write('## 2nd Half Injury Time Analysis')
st.write(f"**t-statistic for Expected 2nd IT vs Updated 2nd IT Overall:** {t_statistic_2nd_overall:.2f}")
st.write(f"**p-value:** {p_value_2nd_overall:.4f}")