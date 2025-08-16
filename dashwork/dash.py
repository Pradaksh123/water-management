import streamlit as st
import plotly.express as px
import pandas as pd
from supabase import create_client
import os
from datetime import datetime, timedelta

# Page config
st.set_page_config(page_title="Water Dashboard", page_icon="💧", layout="wide")

# CSS styling
st.markdown("""
<style>
    .metric-card {
        background: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .header {
        color: #1f77b4;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

def fetch_daily_data():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))
    
    # Get last 30 days of data
    end_date = datetime(2025, 7, 1)
    start_date = end_date - timedelta(days=30)
    
    flow_data = supabase.table('flow_rate') \
        .select('timestamp, location_name, totalizer') \
        .gte('timestamp', start_date.isoformat()) \
        .lte('timestamp', end_date.isoformat()) \
        .execute()
    
    quality_data = supabase.table('water_quality') \
        .select('timestamp, parameter_name, value, location_name') \
        .gte('timestamp', start_date.isoformat()) \
        .lte('timestamp', end_date.isoformat()) \
        .execute()
    
    return pd.DataFrame(flow_data.data), pd.DataFrame(quality_data.data)

def show_dashboard():
    st.title("🌊 Water Management Dashboard")
    st.markdown('<div class="header">June 2025 Performance Overview</div>', unsafe_allow_html=True)
    
    flow_df, quality_df = fetch_daily_data()
    
    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown('<div class="metric-card">📊 <b>Total Flow</b><br>'
                   f'{flow_df["totalizer"].sum():,.0f} L</div>', unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="metric-card">🔍 <b>Avg. Quality Score</b><br>'
                   f'{quality_df["value"].mean():.1f}/100</div>', unsafe_allow_html=True)
    with col3:
        st.markdown('<div class="metric-card">⚠️ <b>Alerts</b><br>'
                   f'{len(quality_df[quality_df["value"] > 1000])} Critical</div>', unsafe_allow_html=True)
    with col4:
        st.markdown('<div class="metric-card">🏭 <b>Active Locations</b><br>'
                   f'{flow_df["location_name"].nunique()} Sites</div>', unsafe_allow_html=True)
    
    st.divider()
    
    # Main charts
    tab1, tab2, tab3 = st.tabs(["📈 Flow Trends", "🧪 Quality Analysis", "📍 Location View"])
    
    with tab1:
        flow_df['date'] = pd.to_datetime(flow_df['timestamp']).dt.date
        daily_flow = flow_df.groupby(['date', 'location_name'])['totalizer'].sum().reset_index()
        
        fig = px.line(daily_flow, x='date', y='totalizer', color='location_name',
                     title='Daily Water Flow Trends', 
                     labels={'totalizer': 'Flow Rate (L)', 'date': 'Date'},
                     height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        quality_df['date'] = pd.to_datetime(quality_df['timestamp']).dt.date
        param_dist = quality_df['parameter_name'].value_counts().reset_index()
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(param_dist, values='count', names='parameter_name',
                         title='Parameter Distribution', hole=0.4)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.box(quality_df, x='parameter_name', y='value',
                        title='Parameter Value Distribution')
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        loc_summary = quality_df.groupby('location_name')['value'].mean().reset_index()
        fig = px.bar(loc_summary, x='location_name', y='value',
                    title='Average Parameter Values by Location',
                    color='location_name')
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    show_dashboard()