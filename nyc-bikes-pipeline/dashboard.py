import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(page_title="NYC Bikes Dashboard", layout="wide")
st.title(" NYC Bikes - Real-Time Analytics Dashboard")

@st.cache_resource(ttl=300)
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host='db',
            database='nyc_bikes',
            user='bikes_user',
            password='bikes_pass',
            connect_timeout=10
        )
        return conn
    except Exception as e:
        st.error(f"Cannot connect to database: {e}")
        return None

def run_query(query, conn):
    """Ejecutar query de forma segura"""
    try:
        if conn and not conn.closed:
            return pd.read_sql(query, conn)
        else:
            st.error("Database connection is closed")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

#conexion
conn = get_db_connection()

if conn is None:
    st.stop()

try:
    #cards
    st.header("System Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_df = run_query("SELECT COUNT(*) as count FROM trip_metadata", conn)
        total = total_df['count'].iloc[0] if not total_df.empty else 0
        st.metric("Total Trips", f"{total:,}")
    
    with col2:
        avg_quality_df = run_query("SELECT AVG(quality_score) as avg FROM trip_metadata", conn)
        avg_quality = avg_quality_df['avg'].iloc[0] if not avg_quality_df.empty else 0
        st.metric("Avg Quality Score", f"{avg_quality:.1f}")
    
    with col3:
        valid_df = run_query("SELECT COUNT(*) as count FROM trip_metadata WHERE is_valid = true", conn)
        valid = valid_df['count'].iloc[0] if not valid_df.empty else 0
        st.metric("Valid Trips", f"{valid:,}")
    
    with col4:
        invalid_df = run_query("SELECT COUNT(*) as count FROM trip_metadata WHERE is_valid = false", conn)
        invalid = invalid_df['count'].iloc[0] if not invalid_df.empty else 0
        st.metric("Invalid Trips", f"{invalid:,}")

    
    st.header("Data Quality Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        #distribucion de los scores
        score_dist = run_query("""
            SELECT 
                FLOOR(quality_score/10)*10 as score_range,
                COUNT(*) as count
            FROM trip_metadata 
            GROUP BY FLOOR(quality_score/10)*10
            ORDER BY score_range
        """, conn)
        
        if not score_dist.empty:
            score_dist['score_range'] = score_dist['score_range'].astype(str) + '-' + (score_dist['score_range'] + 10).astype(str)
            fig = px.bar(score_dist, x='score_range', y='count', 
                        title='Quality Score Distribution',
                        labels={'score_range': 'Score Range', 'count': 'Number of Trips'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No quality score data available")

    with col2:
        #comparacion de porcentajes de validos e invalidos
        valid_stats = run_query("""
            SELECT 
                CASE WHEN is_valid THEN 'Valid' ELSE 'Invalid' END as status,
                COUNT(*) as count
            FROM trip_metadata 
            GROUP BY is_valid
        """, conn)
        
        if not valid_stats.empty:
            fig = px.pie(valid_stats, values='count', names='status', 
                        title='Valid vs Invalid Trips',
                        color='status',
                        color_discrete_map={'Valid': '#00CC96', 'Invalid': '#EF553B'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No validity data available")


    st.header("Performance Trends")
    
    #datos x hora
    hourly_data = run_query("""
        SELECT 
            DATE_TRUNC('hour', processed_at) as hour, 
            COUNT(*) as trips,
            AVG(quality_score) as avg_quality
        FROM trip_metadata 
        WHERE processed_at >= NOW() - INTERVAL '24 hours'
        GROUP BY DATE_TRUNC('hour', processed_at)
        ORDER BY hour
    """, conn)
    
    if not hourly_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(hourly_data, x='hour', y='trips', 
                         title='Trips Volume (Last 24 Hours)',
                         labels={'hour': 'Time', 'trips': 'Number of Trips'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.line(hourly_data, x='hour', y='avg_quality',
                         title='Quality Score Trend (Last 24 Hours)',
                         labels={'hour': 'Time', 'avg_quality': 'Quality Score'})
            fig.add_hline(y=60, line_dash="dash", line_color="red", 
                         annotation_text="Minimum Quality (60)")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hourly trend data available")


    st.header("Bike Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        #tipos bicis
        bike_types = run_query("""
            SELECT 
                bike_type,
                COUNT(*) as count
            FROM trip_metadata 
            GROUP BY bike_type
            ORDER BY count DESC
        """, conn)
        
        if not bike_types.empty:
            fig = px.bar(bike_types, x='bike_type', y='count', 
                        title='Trips by Bike Type')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No bike type data available")

    with col2:
        #miembros vs casuales
        member_stats = run_query("""
            SELECT 
                member_casual,
                COUNT(*) as count
            FROM trip_metadata 
            WHERE member_casual IS NOT NULL
            GROUP BY member_casual
            ORDER BY count DESC
        """, conn)
        
        if not member_stats.empty:
            fig = px.pie(member_stats, values='count', names='member_casual', 
                        title='Member vs Casual Riders')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No member data available")


    st.header("Recent Activity")
    
    recent_trips = run_query("""
        SELECT 
            trip_id,
            bike_type,
            quality_score,
            is_valid,
            processed_at
        FROM trip_metadata 
        ORDER BY processed_at DESC 
        LIMIT 10
    """, conn)
    
    if not recent_trips.empty:
        #display formatear
        display_data = recent_trips.copy()
        display_data['processed_at'] = pd.to_datetime(display_data['processed_at']).dt.strftime('%m/%d %H:%M:%S')
        display_data['Status'] = display_data['is_valid'].map({True: 'Valid', False: 'Invalid'})
        display_data['Quality'] = display_data['quality_score'].round(1)
        
        st.dataframe(display_data[['trip_id', 'bike_type', 'Quality', 'Status', 'processed_at']], 
                    use_container_width=True,
                    column_config={
                        "trip_id": "Trip ID",
                        "bike_type": "Bike Type", 
                        "Quality": "Quality Score",
                        "Status": "Status",
                        "processed_at": "Processed At"
                    })
    else:
        st.info("No recent trips data available")

except Exception as e:
    st.error(f"Application error: {e}")

finally:
    #cerrar conexion
    try:
        if conn and not conn.closed:
            conn.close()
    except:
        pass