import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

st.set_page_config(page_title="NYC Bikes Dashboard", layout="wide")
st.title("NYC Bikes Analytics")

@st.cache_resource
def get_db_connection():
    return psycopg2.connect(
        host='db',
        database='nyc_bikes',
        user='bikes_user',
        password='bikes_pass'
    )

try:
    conn = get_db_connection()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total = pd.read_sql("SELECT COUNT(*) as count FROM trip_metadata", conn)['count'].iloc[0]
        st.metric("Total Trips", f"{total:,}")
    
    with col2:
        avg_quality = pd.read_sql("SELECT AVG(quality_score) as avg FROM trip_metadata", conn)['avg'].iloc[0]
        st.metric("Avg Quality", f"{avg_quality:.1f}")
    
    with col3:
        valid = pd.read_sql("SELECT COUNT(*) as count FROM trip_metadata WHERE is_valid = true", conn)['count'].iloc[0]
        st.metric("Valid Trips", f"{valid:,}")
    
    hourly = pd.read_sql("""
        SELECT DATE_TRUNC('hour', ingested_at) as hour, 
               COUNT(*) as trips,
               AVG(quality_score) as quality
        FROM trip_metadata 
        WHERE ingested_at >= NOW() - INTERVAL '24 hours'
        GROUP BY DATE_TRUNC('hour', ingested_at)
        ORDER BY hour
    """, conn)
    
    if not hourly.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(hourly, x='hour', y='trips', title='Trips per Hour')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.line(hourly, x='hour', y='quality', title='Quality Score Trend')
            st.plotly_chart(fig, use_container_width=True)
    
    quality_dist = pd.read_sql("""
        SELECT quality_band, COUNT(*) as count
        FROM trip_metadata 
        GROUP BY quality_band
    """, conn)
    
    if not quality_dist.empty:
        fig = px.pie(quality_dist, values='count', names='quality_band', title='Quality Distribution')
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Database error: {e}")