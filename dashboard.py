import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
from load import fn_get_db_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Page Config
st.set_page_config(
    page_title="Cambridge Crime Trends Dashboard",
    page_icon="🚓",
    layout="wide"
)

# ----------------------------------------------------------------------------
# Database Connection & Data Loading
# ----------------------------------------------------------------------------
@st.cache_data(ttl=300) # Cache data for 5 minutes
def load_data():
    """
    Fetches crime incident data from the PostgreSQL database.
    """
    engine = fn_get_db_engine()
    if engine is None:
        st.error("Failed to connect to the database.")
        return pd.DataFrame()

    try:
        query = "SELECT * FROM cpd_db.tb_cpd_incidents"
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        # Ensure date_time is datetime object
        if 'date_time' in df.columns:
            df['date_time'] = pd.to_datetime(df['date_time'])
            
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Load Data
with st.spinner('Loading data from database...'):
    df = load_data()

if df.empty:
    st.warning("No data available. Please run the ETL pipeline first to populate the database.")
    st.stop()

# ----------------------------------------------------------------------------
# Sidebar Filters
# ----------------------------------------------------------------------------
st.sidebar.header("Filters")

# Date Range Filter
min_date = df['date_time'].min().date()
max_date = df['date_time'].max().date()

start_date, end_date = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Crime Type Filter
crime_types = sorted(df['type'].dropna().unique())
selected_types = st.sidebar.multiselect(
    "Select Crime Types",
    options=crime_types,
    default=crime_types[:5] if len(crime_types) > 5 else crime_types 
)

# Apply Filters
mask = (
    (df['date_time'].dt.date >= start_date) & 
    (df['date_time'].dt.date <= end_date) & 
    (df['type'].isin(selected_types))
)
filtered_df = df[mask]

# ----------------------------------------------------------------------------
# KPI Metrics
# ----------------------------------------------------------------------------
st.title("🚓 Cambridge Crime Trends Dashboard")

col1, col2, col3 = st.columns(3)

total_incidents = len(filtered_df)
num_days = (end_date - start_date).days + 1
avg_daily_incidents = total_incidents / num_days if num_days > 0 else 0

col1.metric("Total Incidents", f"{total_incidents:,}")
col2.metric("Avg Daily Incidents", f"{avg_daily_incidents:.1f}")
col3.metric("Selected Date Range", f"{num_days} Days")

st.markdown("---")

# ----------------------------------------------------------------------------
# Visualizations
# ----------------------------------------------------------------------------

# Row 1: Time Series & Top Crime Types
row1_col1, row1_col2 = st.columns(2)

with row1_col1:
    st.subheader("Incidents Over Time")
    if not filtered_df.empty:
        # Aggregate by Day
        daily_counts = filtered_df.resample('D', on='date_time').size().reset_index(name='count')
        fig_time = px.line(daily_counts, x='date_time', y='count', title='Daily Trend')
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("No data to display for current selection.")

with row1_col2:
    st.subheader("Top Crime Types")
    if not filtered_df.empty:
        type_counts = filtered_df['type'].value_counts().reset_index()
        type_counts.columns = ['Crime Type', 'Count']
        fig_type = px.bar(type_counts.head(10), x='Count', y='Crime Type', orientation='h', title='Top 10 Crime Types', color='Count')
        st.plotly_chart(fig_type, use_container_width=True)
    else:
        st.info("No data to display for current selection.")

# Row 2: Top Locations (and Day of Week if interesting)
st.subheader("Top Locations")
if not filtered_df.empty:
    loc_counts = filtered_df['location'].value_counts().head(10).reset_index()
    loc_counts.columns = ['Location', 'Count']
    fig_loc = px.bar(loc_counts, x='Location', y='Count', title='Top 10 Incident Locations')
    st.plotly_chart(fig_loc, use_container_width=True)

# ----------------------------------------------------------------------------
# Raw Data View
# ----------------------------------------------------------------------------
with st.expander("View Raw Data"):
    st.dataframe(filtered_df.sort_values(by='date_time', ascending=False))
