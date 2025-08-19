import streamlit as st 
import plotly.express as px 
import pandas as pd 
from supabase import create_client 
import os 
from datetime import datetime, timedelta 
from dotenv import load_dotenv
from app.config import CONFIG
import time

# Initialize environment variables
load_dotenv()

class DashboardManager:
    def __init__(self):
        self.supabase = None
        self.initialized = False
        self.init_error = None

    def initialize(self):
        """Initialize the dashboard components with error handling"""
        try:
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_ANON_KEY")
            
            if not supabase_url or not supabase_key:
                raise ValueError("Supabase credentials not configured")
                
            self.supabase = create_client(supabase_url, supabase_key)
            self.initialized = True
        except Exception as e:
            self.init_error = str(e)
            self.initialized = False

    def fetch_data_with_pagination(self, table_name, start_date=None, end_date=None):
        """Fetch paginated data with error handling"""
        if not self.initialized:
            return pd.DataFrame()
            
        all_data = []
        page_size = 1000
        offset = 0
        
        try:
            while True:
                query = self.supabase.table(table_name).select('*')
                
                if start_date:
                    query = query.gte('timestamp', start_date.isoformat())
                if end_date:
                    query = query.lte('timestamp', end_date.isoformat())
                    
                result = query.range(offset, offset + page_size - 1).execute()
                page_data = result.data
                
                if not page_data:
                    break
                    
                all_data.extend(page_data)
                offset += page_size
                
                if len(page_data) < page_size:
                    break
                    
            return pd.DataFrame(all_data)
        except Exception as e:
            st.error(f"Data fetch error: {str(e)}")
            return pd.DataFrame()

    def fetch_split_data(self, mode='historical'):
        """Fetch and split data with error handling"""
        if not self.initialized:
            return pd.DataFrame(), pd.DataFrame()
            
        try:
            all_flow_df = self.fetch_data_with_pagination('flow_rate')
            all_quality_df = self.fetch_data_with_pagination('water_quality')
            
            if mode == 'historical':
                flow_split_index = int(len(all_flow_df) * 0.8)
                quality_split_index = int(len(all_quality_df) * 0.8)
                flow_df = all_flow_df.iloc[:flow_split_index]
                quality_df = all_quality_df.iloc[:quality_split_index]
            elif mode == 'live':
                flow_split_index = int(len(all_flow_df) * 0.8)
                quality_split_index = int(len(all_quality_df) * 0.8)
                flow_df = all_flow_df.iloc[flow_split_index:]
                quality_df = all_quality_df.iloc[quality_split_index:]
            else:
                flow_df = all_flow_df
                quality_df = all_quality_df
            
            # Clean data
            if not flow_df.empty:
                flow_df['totalizer'] = pd.to_numeric(flow_df['totalizer'], errors='coerce')
            if not quality_df.empty:
                quality_df['value'] = pd.to_numeric(quality_df['value'], errors='coerce')
                
            return flow_df, quality_df
            
        except Exception as e:
            st.error(f"Data processing error: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()

    def fetch_recent_data(self, hours=24):
        """Fetch recent data with error handling"""
        if not self.initialized:
            return pd.DataFrame(), pd.DataFrame()
            
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            quality_df = self.fetch_data_with_pagination('water_quality', start_time, end_time)
            flow_df = self.fetch_data_with_pagination('flow_rate', start_time, end_time)

            if not quality_df.empty:
                quality_df['value'] = pd.to_numeric(quality_df['value'], errors='coerce')
                quality_df.sort_values(by='timestamp', ascending=False, inplace=True)
                
            if not flow_df.empty:
                flow_df['totalizer'] = pd.to_numeric(flow_df['totalizer'], errors='coerce')
                flow_df.sort_values(by='timestamp', ascending=False, inplace=True)
                
            return quality_df, flow_df
        except Exception as e:
            st.error(f"Recent data fetch error: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()

def show_dashboard():
    """Display the main dashboard"""
    if "dashboard_manager" not in st.session_state:
        st.session_state.dashboard_manager = DashboardManager()
        with st.spinner("Initializing dashboard..."):
            st.session_state.dashboard_manager.initialize()
        
        if not st.session_state.dashboard_manager.initialized:
            st.error(f"Dashboard initialization failed: {st.session_state.dashboard_manager.init_error}")
            return
    
    dm = st.session_state.dashboard_manager
    flow_df, quality_df = dm.fetch_split_data(mode='historical')
    
    if flow_df.empty and quality_df.empty:
        st.warning("No data available. Please check your database connection.")
        return
    
    # Dashboard title and header
    st.title("🌊 Water Management Dashboard")
    st.markdown('<div class="header">Performance Overview</div>', unsafe_allow_html=True)
    
    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_flow = flow_df["totalizer"].sum() if not flow_df.empty and "totalizer" in flow_df.columns else 0
        st.markdown(f'<div class="metric-card">📊 <b>Total Flow</b><br>{total_flow:,.0f} L</div>', unsafe_allow_html=True)
    with col2:
        avg_quality = quality_df["value"].mean() if not quality_df.empty and "value" in quality_df.columns else 0
        st.markdown(f'<div class="metric-card">🔍 <b>Avg. Quality Score</b><br>{avg_quality:.1f}</div>', unsafe_allow_html=True)
    with col3:
        alerts_count = len(quality_df[quality_df["value"] > 1000]) if not quality_df.empty and "value" in quality_df.columns else 0
        st.markdown(f'<div class="metric-card">⚠️ <b>Alerts</b><br>{alerts_count} Critical</div>', unsafe_allow_html=True)
    with col4:
        active_locations = flow_df["location_name"].nunique() if not flow_df.empty and "location_name" in flow_df.columns else 0
        st.markdown(f'<div class="metric-card">🏭 <b>Active Locations</b><br>{active_locations} Sites</div>', unsafe_allow_html=True)
    
    st.divider()
    
    # Main charts
    tab1, tab2, tab3 = st.tabs(["📈 Flow Trends", "🧪 Quality Analysis", "📍 Location View"])
    
    with tab1:
        if not flow_df.empty and "timestamp" in flow_df.columns and "location_name" in flow_df.columns:
            flow_df['date'] = pd.to_datetime(flow_df['timestamp'], format='mixed').dt.date
            daily_flow = flow_df.groupby(['date', 'location_name'])['totalizer'].sum().reset_index()
            
            fig = px.line(daily_flow, x='date', y='totalizer', color='location_name',
                        title='Daily Water Flow Trends',
                        labels={'totalizer': 'Flow Rate (L)', 'date': 'Date'},
                        height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No flow data available")
    
    with tab2:
        if not quality_df.empty and "parameter_name" in quality_df.columns:
            st.markdown("#### Quality Parameter Analysis")
            quality_df_clean = quality_df.dropna(subset=['value'])
            
            selected_params = st.multiselect(
                "Select Parameters for Analysis",
                quality_df_clean['parameter_name'].unique(),
                default=quality_df_clean['parameter_name'].unique()[:3] if not quality_df_clean.empty else []
            )
            
            filtered_quality_df = quality_df_clean[quality_df_clean['parameter_name'].isin(selected_params)]
            
            if not filtered_quality_df.empty:
                quality_df_clean['date'] = pd.to_datetime(quality_df_clean['timestamp'], format='mixed').dt.date
                param_dist = filtered_quality_df['parameter_name'].value_counts().reset_index()
                
                col1, col2 = st.columns(2)
                with col1:
                    fig = px.pie(param_dist, values='count', names='parameter_name',
                                title='Parameter Distribution', hole=0.4)
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    fig = px.box(filtered_quality_df, x='parameter_name', y='value',
                                title='Parameter Value Distribution')
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available for the selected parameters.")
        else:
            st.info("No quality data available")
    
    with tab3:
        if not flow_df.empty and "location_name" in flow_df.columns:
            st.markdown("#### Location-based Flow Analysis")
            selected_locations = st.multiselect(
                "Select Locations for Analysis",
                flow_df['location_name'].unique(),
                default=flow_df['location_name'].unique()
            )
            
            filtered_flow_df = flow_df[flow_df['location_name'].isin(selected_locations)]
            
            if not filtered_flow_df.empty and "totalizer" in filtered_flow_df.columns:
                loc_summary = filtered_flow_df.groupby('location_name')['totalizer'].mean().reset_index()
                loc_summary.columns = ['location_name', 'avg_value']
                fig = px.bar(loc_summary, x='location_name', y='avg_value',
                            title='Average Flow Values by Location',
                            color='location_name')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available for the selected locations.")
        else:
            st.info("No location data available")

def show_alerts():
    """Display the alerts monitoring panel"""
    if "dashboard_manager" not in st.session_state:
        st.session_state.dashboard_manager = DashboardManager()
        with st.spinner("Initializing dashboard..."):
            st.session_state.dashboard_manager.initialize()
        
        if not st.session_state.dashboard_manager.initialized:
            st.error(f"Dashboard initialization failed: {st.session_state.dashboard_manager.init_error}")
            return
    
    dm = st.session_state.dashboard_manager
    
    st.markdown("### 🚨 Real-time Alert Monitoring")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        current_time = datetime.now().strftime("%H:%M:%S")
        st.markdown(f"**🕐 Live Time:** {current_time}")
    
    with col2:
        if st.button("🔄 Refresh Now", use_container_width=True):
            st.rerun()
    
    with col3:
        auto_refresh = st.checkbox("🔃 Auto-refresh (10s)", value=False, key="alerts_auto_refresh")
    
    st.divider()
    
    quality_df, _ = dm.fetch_recent_data()
    
    if quality_df.empty:
        st.warning("No recent data available for alert monitoring.")
        return
    
    quality_df['value'] = pd.to_numeric(quality_df['value'], errors='coerce')
    quality_df.dropna(subset=['value'], inplace=True)
    
    current_alerts = []
    
    for _, row in quality_df.iterrows():
        param = row['parameter_name']
        value = row['value']
        timestamp = pd.to_datetime(row['timestamp'], format='mixed')
        
        if param in CONFIG["SAFE_RANGES"]:
            min_val, max_val = CONFIG["SAFE_RANGES"][param]
            if not (min_val <= value <= max_val):
                current_alerts.append({
                    'timestamp': timestamp,
                    'parameter': param,
                    'value': value,
                    'safe_min': min_val,
                    'safe_max': max_val
                })
    
    total_alerts = len(current_alerts)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🚨 Total Alerts</h3>
            <h2 style="color: {'red' if total_alerts > 0 else 'green'}">{total_alerts}</h2>
            <p>Out-of-range parameters</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3>✅ Normal</h3>
            <h2 style="color: green">{len(quality_df) - total_alerts}</h2>
            <p>Within safe range</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    if current_alerts:
        st.markdown("#### 🚨 Current Out-of-Range Parameters")
        for alert in current_alerts:
            time_str = alert['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
            st.warning(f"""
            **{alert['parameter']}**: {alert['value']:.2f}   
            **Safe Range**: {alert['safe_min']}-{alert['safe_max']}   
            **Time**: {time_str}
            """)
    else:
        st.success("🎉 All parameters are within safe ranges")
    
    if auto_refresh:
        time.sleep(10)
        st.rerun()

# For testing the component standalone
if __name__ == "__main__":
    show_dashboard()
    # show_alerts()  # Uncomment to test alerts view separately