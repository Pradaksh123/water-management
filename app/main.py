import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from supabase import create_client
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import random

# --- Load environment variables ---
load_dotenv()

# --- Page configuration ---
st.set_page_config(
    page_title="Water Management System",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuration ---
CONFIG = {
    "SUPABASE_URL": os.getenv("SUPABASE_URL"),
    "SUPABASE_KEY": os.getenv("SUPABASE_ANON_KEY"),
    "GEMINI_API_KEY": os.getenv("GEMINI_API_KEY"),
    "TABLES": {
        "quality": "water_quality",
        "flow": "flow_rate"
    },
    "PARAMETERS": [
        "HUMIDITY", "ETP (TDS)", "ETP (pH)",
        "STP (TDS)", "STP (TSS)", "STP (BOD)",
        "STP (pH)", "STP (COD)"
    ],
    "LOCATIONS": [
        "Corporation Water",
        "Ground Water Source 1",
        "Ground Water Source 2",
        "Industrial Process",
        "Tanker Water Supply"
    ],
    "SAFE_RANGES": {
        "HUMIDITY": (30, 70),
        "ETP (TDS)": (100, 1000),
        "ETP (pH)": (6.5, 9.0),
        "STP (TDS)": (100, 1000),
        "STP (TSS)": (1000, 3000),
        "STP (BOD)": (0, 5),
        "STP (pH)": (6.5, 9.0),
        "STP (COD)": (1000, 3000)
    }
}

# --- CSS styling with light blue header ---
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        color: lightblue;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
        text-align: center;
    }
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
    .alert-card {
        background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
    }
    .panel-header {
        color: #1e3c72;
        border-bottom: 3px solid #2a5298;
        padding-bottom: 10px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# --- Data fetching functions ---
@st.cache_data(ttl=60)
def fetch_data_with_pagination(table_name, _supabase_client, start_date=None, end_date=None):
    all_data = []
    page_size = 1000
    offset = 0
    
    while True:
        query = _supabase_client.table(table_name).select('*')
        
        if start_date:
            query = query.gte('timestamp', start_date.isoformat())
        if end_date:
            query = query.lte('timestamp', end_date.isoformat())
            
        query = query.range(offset, offset + page_size - 1).execute()
        
        page_data = query.data
        if not page_data:
            break
        
        all_data.extend(page_data)
        offset += page_size
        
        if len(page_data) < page_size:
            break
            
    return pd.DataFrame(all_data)

@st.cache_data(ttl=60)
def fetch_daily_data():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))
    
    end_date = datetime(2025, 7, 1)
    start_date = end_date - timedelta(days=30)
    
    flow_df = fetch_data_with_pagination('flow_rate', supabase, start_date, end_date)
    quality_df = fetch_data_with_pagination('water_quality', supabase, start_date, end_date)
    
    if not flow_df.empty:
        flow_df['totalizer'] = pd.to_numeric(flow_df['totalizer'], errors='coerce')
    
    if not quality_df.empty:
        quality_df['value'] = pd.to_numeric(quality_df['value'], errors='coerce')
    
    return flow_df, quality_df

@st.cache_data(ttl=30)
def fetch_recent_data(hours=24):
    try:
        supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        quality_df = fetch_data_with_pagination('water_quality', supabase, start_time, end_time)
        flow_df = fetch_data_with_pagination('flow_rate', supabase, start_time, end_time)

        if not quality_df.empty:
            quality_df['value'] = pd.to_numeric(quality_df['value'], errors='coerce')
            quality_df.sort_values(by='timestamp', ascending=False, inplace=True)
            
        if not flow_df.empty:
            flow_df['totalizer'] = pd.to_numeric(flow_df['totalizer'], errors='coerce')
            flow_df.sort_values(by='timestamp', ascending=False, inplace=True)
        
        return quality_df, flow_df
    except Exception as e:
        st.error(f"Failed to fetch recent data: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- Updated Chatbot class ---
class WaterChatBot:
    def __init__(self):
        try:
            self.supabase = create_client(CONFIG["SUPABASE_URL"], CONFIG["SUPABASE_KEY"])
            
            self.llm = ChatGoogleGenerativeAI(
                model="gemini-1.5-flash",
                google_api_key=CONFIG["GEMINI_API_KEY"]
            )
            
            self.prompt_template = ChatPromptTemplate.from_messages([
                ("system", """
                You are a water management expert analyzing data from June 1 to July 1 2025.
                Current alerts: {alerts}
                
                Strict Rules:
                1. Only use data from the provided CSV (from Supabase database)
                2. Never mention location for water quality parameters (location data not available)
                3. For flow data, you may mention location when available
                4. Highlight values outside safe ranges
                5. Never invent data outside June-July 2025
                6. If asked about location for water quality, state that this data is not available
                """),
                ("human", """
                Question: {question}
                
                Data (CSV):
                {context}
                """)
            ])
            self.chain = self.prompt_template | self.llm | StrOutputParser()
        except Exception as e:
            st.error(f"Failed to initialize chatbot: {str(e)}")
            self.supabase = None
            self.llm = None

    def fetch_data(self, table, params=None, locations=None):
        if not self.supabase:
            return pd.DataFrame()
            
        end_date = datetime(2025, 7, 1)
        start_date = datetime(2025, 6, 1)
        df = fetch_data_with_pagination(table, self.supabase, start_date, end_date)

        if params and not df.empty and 'parameter_name' in df.columns:
            df = df[df['parameter_name'].isin(params)]
        
        if locations and not df.empty and 'location_name' in df.columns:
            df = df[df['location_name'].isin(locations)]

        return df

    def check_alerts(self):
        alerts = []
        df = self.fetch_data(CONFIG["TABLES"]["quality"])
        
        if not df.empty:
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df.dropna(subset=['value'], inplace=True)

            for _, row in df.iterrows():
                param = row['parameter_name']
                min_val, max_val = CONFIG["SAFE_RANGES"].get(param, (None, None))
                if min_val and (row['value'] < min_val or row['value'] > max_val):
                    alerts.append(f"{param}: {row['value']} (Safe: {min_val}-{max_val})")
        return alerts

    def generate_response(self, question):
        if not self.llm or not self.supabase:
            return "❌ Chatbot is not properly initialized. Please check your API keys."
            
        alerts = self.check_alerts()
        alert_status = "✅ All parameters normal" if not alerts else "\n".join(alerts)
        
        if "flow" in question.lower():
            df = self.fetch_data(CONFIG["TABLES"]["flow"])
        else:
            params = [p for p in CONFIG["PARAMETERS"] if p.lower() in question.lower()]
            df = self.fetch_data(CONFIG["TABLES"]["quality"], params=params if params else None)
        
        if df.empty:
            return "No relevant data found for your query."
        
        return self.chain.invoke({
            "question": question,
            "context": df.to_csv(),
            "alerts": alert_status
        })

# --- Dashboard functions ---
def show_dashboard():
    st.title("🌊 Water Management Dashboard")
    st.markdown('<div class="header">June 2025 Performance Overview</div>', unsafe_allow_html=True)
    
    flow_df, quality_df = fetch_daily_data()
    
    if flow_df.empty and quality_df.empty:
        st.warning("No data available. Please check your database connection.")
        return
    
    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_flow = flow_df["totalizer"].sum() if not flow_df.empty else 0
        st.markdown(f'<div class="metric-card">📊 <b>Total Flow</b><br>{total_flow:,.0f} L</div>', unsafe_allow_html=True)
    with col2:
        avg_quality = quality_df["value"].mean() if not quality_df.empty else 0
        st.markdown(f'<div class="metric-card">🔍 <b>Avg. Quality Score</b><br>{avg_quality:.1f}</div>', unsafe_allow_html=True)
    with col3:
        alerts_count = len(quality_df[quality_df["value"] > 1000]) if not quality_df.empty else 0
        st.markdown(f'<div class="metric-card">⚠️ <b>Alerts</b><br>{alerts_count} Critical</div>', unsafe_allow_html=True)
    with col4:
        active_locations = flow_df["location_name"].nunique() if not flow_df.empty else 0
        st.markdown(f'<div class="metric-card">🏭 <b>Active Locations</b><br>{active_locations} Sites</div>', unsafe_allow_html=True)
    
    st.divider()
    
    # Main charts
    tab1, tab2, tab3 = st.tabs(["📈 Flow Trends", "🧪 Quality Analysis", "📍 Location View"])
    
    with tab1:
        if not flow_df.empty:
            flow_df['date'] = pd.to_datetime(flow_df['timestamp']).dt.date
            daily_flow = flow_df.groupby(['date', 'location_name'])['totalizer'].sum().reset_index()
            
            fig = px.line(daily_flow, x='date', y='totalizer', color='location_name',
                          title='Daily Water Flow Trends', 
                          labels={'totalizer': 'Flow Rate (L)', 'date': 'Date'},
                          height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No flow data available")
    
    with tab2:
        if not quality_df.empty:
            st.markdown("#### Quality Parameter Analysis")
            quality_df_clean = quality_df.dropna(subset=['value'])
            
            selected_params = st.multiselect(
                "Select Parameters for Analysis",
                quality_df_clean['parameter_name'].unique(),
                default=quality_df_clean['parameter_name'].unique()[:3] if not quality_df_clean.empty else []
            )
            
            filtered_quality_df = quality_df_clean[quality_df_clean['parameter_name'].isin(selected_params)]
            
            if not filtered_quality_df.empty:
                quality_df_clean['date'] = pd.to_datetime(quality_df_clean['timestamp']).dt.date
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
        if not flow_df.empty:
            st.markdown("#### Location-based Flow Analysis")
            selected_locations = st.multiselect(
                "Select Locations for Analysis",
                flow_df['location_name'].unique(),
                default=flow_df['location_name'].unique()
            )
            
            filtered_flow_df = flow_df[flow_df['location_name'].isin(selected_locations)]
            
            if not filtered_flow_df.empty:
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

# --- Analytics panel ---
def show_analytics():
    st.markdown("### 📊 Advanced Analytics")
    
    try:
        flow_df, quality_df = fetch_daily_data()
        
        if quality_df.empty and flow_df.empty:
            st.warning("No data available for analytics.")
            return
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            quality_df_clean = quality_df.dropna(subset=['value'])
            parameters = quality_df_clean['parameter_name'].unique() if not quality_df_clean.empty else []
            selected_params = st.multiselect(
                "Select Parameters",
                parameters,
                default=parameters[:3] if len(parameters) > 3 else parameters
            )
        
        with col2:
            locations = flow_df['location_name'].unique() if not flow_df.empty else []
            selected_locations = st.multiselect(
                "Select Locations",
                locations,
                default=locations
            )
        
        with col3:
            time_range = st.selectbox("Time Range", ["Last 7 days", "Last 30 days", "All data"])
        
        filtered_quality_df = quality_df_clean[quality_df_clean['parameter_name'].isin(selected_params)] if selected_params else quality_df_clean
        filtered_flow_df = flow_df[flow_df['location_name'].isin(selected_locations)] if selected_locations else flow_df
        
        st.divider()
        
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Trends", "📊 Statistics", "🔗 Correlations", "📍 Location Comparison"])
        
        with tab1:
            if not filtered_quality_df.empty:
                st.markdown("#### Parameter Trends Over Time")
                filtered_quality_df['datetime'] = pd.to_datetime(filtered_quality_df['timestamp'])
                fig = px.line(filtered_quality_df, x='datetime', y='value', color='parameter_name',
                              title='Selected Parameter Trends Over Time')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available for trend analysis")
        
        with tab2:
            if not filtered_quality_df.empty:
                st.markdown("#### Statistical Summary")
                stats_summary = filtered_quality_df.groupby('parameter_name')['value'].agg([
                    'count', 'mean', 'std', 'min', 'max', 'median'
                ]).round(2)
                stats_summary.columns = ['Count', 'Mean', 'Std Dev', 'Min', 'Max', 'Median']
                st.dataframe(stats_summary, use_container_width=True)
                
                st.markdown("#### Compliance Analysis")
                compliance_data = []
                for param in filtered_quality_df['parameter_name'].unique():
                    if param in CONFIG["SAFE_RANGES"]:
                        param_data = filtered_quality_df[filtered_quality_df['parameter_name'] == param]
                        min_val, max_val = CONFIG["SAFE_RANGES"][param]
                        
                        total_readings = len(param_data)
                        compliant_readings = len(param_data[
                            (param_data['value'] >= min_val) & (param_data['value'] <= max_val)
                        ])
                        compliance_rate = (compliant_readings / total_readings * 100) if total_readings > 0 else 0
                        
                        compliance_data.append({
                            'Parameter': param,
                            'Total Readings': total_readings,
                            'Compliant': compliant_readings,
                            'Compliance Rate (%)': round(compliance_rate, 1)
                        })
                
                if compliance_data:
                    compliance_df = pd.DataFrame(compliance_data)
                    st.dataframe(compliance_df, use_container_width=True)
            else:
                st.info("No data available for statistical analysis")
        
        with tab3:
            if not filtered_quality_df.empty and len(filtered_quality_df['parameter_name'].unique()) > 1:
                st.markdown("#### Parameter Correlations")
                pivot_data = filtered_quality_df.pivot_table(
                    index='timestamp',
                    columns='parameter_name',
                    values='value'
                )
                
                if len(pivot_data.columns) > 1:
                    correlation_matrix = pivot_data.corr()
                    fig = px.imshow(
                        correlation_matrix,
                        title="Parameter Correlation Matrix",
                        color_continuous_scale='RdBu',
                        aspect='auto'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Need at least 2 numeric parameters for correlation analysis")
            else:
                st.info("Need more data for correlation analysis")
        
        with tab4:
            st.markdown("#### Parameter-Location Comparison")
            st.warning("Water quality parameters cannot be compared by location as location data is not available in the water_quality table.")
            
            if not filtered_flow_df.empty and selected_locations:
                st.markdown("#### Average Flow Rate by Location")
                loc_summary = filtered_flow_df.groupby('location_name')['totalizer'].mean().reset_index()
                fig = px.bar(loc_summary, x='location_name', y='totalizer', color='location_name',
                             title='Average Flow Values by Location')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Please select at least one location for comparison.")

    except Exception as e:
        st.error(f"Error in analytics: {str(e)}")

# --- Simplified alerts without severity/status_type ---
def show_alerts():
    st.markdown("### 🚨 Real-time Alert Monitoring")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        current_time = datetime.now().strftime("%H:%M:%S")
        st.markdown(f"**🕐 Live Time:** {current_time}")
    
    with col2:
        if st.button("🔄 Refresh Now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    with col3:
        auto_refresh = st.checkbox("🔃 Auto-refresh (10s)", value=False)
    
    st.divider()
    
    quality_df, _ = fetch_recent_data()
    
    if quality_df.empty:
        st.warning("No recent data available for alert monitoring.")
        return
    
    quality_df['value'] = pd.to_numeric(quality_df['value'], errors='coerce')
    quality_df.dropna(subset=['value'], inplace=True)
    
    current_alerts = []
    
    for _, row in quality_df.iterrows():
        param = row['parameter_name']
        value = row['value']
        timestamp = pd.to_datetime(row['timestamp'])
        
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

def show_chatbot(initial_question=None):
    if not all([CONFIG["SUPABASE_URL"], CONFIG["SUPABASE_KEY"], CONFIG["GEMINI_API_KEY"]]):
        st.error("Missing required environment variables for AI chatbot.")
        st.info("Please ensure SUPABASE_URL, SUPABASE_ANON_KEY, and GEMINI_API_KEY are set.")
        return
    
    if "chatbot" not in st.session_state:
        with st.spinner("Initializing AI chatbot..."):
            st.session_state.chatbot = WaterChatBot()
    
    chatbot = st.session_state.chatbot
    
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    
    if initial_question and initial_question not in [msg["content"] for msg in st.session_state.chat_history]:
        st.session_state.chat_history.append({"role": "user", "content": initial_question})
        with st.chat_message("user"):
            st.markdown(initial_question)
        response = chatbot.generate_response(initial_question)
        with st.chat_message("assistant"):
            st.markdown(response)
        st.session_state.chat_history.append({"role": "assistant", "content": response})
    
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
    
    if prompt := st.chat_input("Ask about water data..."):
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                response = chatbot.generate_response(prompt)
                st.markdown(response)
        st.session_state.chat_history.append({"role": "assistant", "content": response})

def main():
    st.markdown("""
    <div class="main-header">
        <h1>🌊 Water Management System</h1>
        <p>Real-time monitoring, analytics, and AI-powered insights for water quality management</p>
    </div>
    """, unsafe_allow_html=True)
    
    with st.sidebar:
        st.markdown("### 🧭 Navigation")
        
        panel = st.selectbox(
            "Select Panel",
            ["Dashboard", "Analytics", "AI Query", "Alerts"],
            key="panel_selector"
        )
        
        st.markdown("---")
        
        st.markdown("### ⚡ Quick Actions")
        
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        
        st.markdown("### 📊 System Status")
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.info(f"**Last Update:** {current_time}")
        
        st.markdown("### 🤖 Quick Queries")
        quick_queries = [
            "Show me the latest water quality alerts",
            "What's the pH trend this week?",
            "Which location has the highest flow?",
            "Flow rate summary for all locations"
        ]
        
        for query in quick_queries:
            if st.button(f"💬 {query}", key=f"quick_{hash(query)}", use_container_width=True):
                st.session_state["panel_selector"] = "AI Query"
                st.session_state["quick_query"] = query
                st.rerun()
    
    with st.container():
        if panel == "Dashboard":
            st.markdown('<h2 class="panel-header">📊 Dashboard</h2>', unsafe_allow_html=True)
            show_dashboard()
            
        elif panel == "Analytics":
            st.markdown('<h2 class="panel-header">📈 Analytics</h2>', unsafe_allow_html=True)
            show_analytics()
            
        elif panel == "AI Query":
            st.markdown('<h2 class="panel-header">🤖 AI Query</h2>', unsafe_allow_html=True)
            initial_query = st.session_state.pop("quick_query", None)
            show_chatbot(initial_question=initial_query)
            
        elif panel == "Alerts":
            st.markdown('<h2 class="panel-header">🚨 Alerts</h2>', unsafe_allow_html=True)
            show_alerts()

if __name__ == "__main__":
    main()