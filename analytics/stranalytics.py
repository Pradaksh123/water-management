import streamlit as st
import plotly.express as px
import pandas as pd
from supabase import create_client
import os
from datetime import datetime

# Page config
st.set_page_config(page_title="Water Analytics", page_icon="📊", layout="wide")

def fetch_analytics_data():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))
    
    # Get all quality data for June 2025
    quality_data = supabase.table('water_quality') \
        .select('timestamp, parameter_name, value, location_name') \
        .gte('timestamp', datetime(2025, 6, 1).isoformat()) \
        .lte('timestamp', datetime(2025, 7, 1).isoformat()) \
        .execute()
    
    return pd.DataFrame(quality_data.data)

def show_analytics():
    st.title("📊 Advanced Water Analytics")
    st.markdown("### Comparative Analysis | June 2025")
    
    df = fetch_analytics_data()
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    
    # Comparison Section
    st.header("🔍 Parameter Comparison")
    
    selected_params = st.multiselect(
        "Select parameters to compare",
        options=df['parameter_name'].unique(),
        default=["ETP (TDS)", "STP (TDS)", "HUMIDITY"]
    )
    
    if selected_params:
        fig = px.line(
            df[df['parameter_name'].isin(selected_params)],
            x='date', y='value', color='parameter_name',
            facet_col='parameter_name', facet_col_wrap=2,
            height=800, title='Daily Parameter Comparison'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Location Analysis
    st.header("🏭 Location Performance")
    
    col1, col2 = st.columns(2)
    with col1:
        location = st.selectbox(
            "Select location",
            options=df['location_name'].unique()
        )
        
        loc_df = df[df['location_name'] == location]
        param_avg = loc_df.groupby('parameter_name')['value'].mean().reset_index()
        
        fig = px.bar_polar(
            param_avg, r='value', theta='parameter_name',
            title=f'Parameter Radar - {location}',
            template="plotly_dark"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.scatter(
            df, x='date', y='value', color='parameter_name',
            hover_data=['location_name'], title='Parameter Correlation'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Flow Rate Meter
    st.divider()
    st.header("🌊 Flow Rate Analysis")
    
    avg_flow = 2450  # Example value - replace with actual query
    st.markdown(f"""
    <div style="background: #f8f9fa; border-radius: 10px; padding: 20px; text-align: center;">
        <h3 style="color: #1f77b4;">Average Daily Flow Rate</h3>
        <div style="font-size: 48px; font-weight: bold; color: #2ca02c;">
            {avg_flow:,.0f} <span style="font-size: 20px;">Liters</span>
        </div>
        <div style="height: 20px; background: #e9ecef; border-radius: 10px; margin: 15px 0;">
            <div style="width: 78%; height: 100%; background: #2ca02c; border-radius: 10px;"></div>
        </div>
        <p style="color: #6c757d;">78% of capacity</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    show_analytics()