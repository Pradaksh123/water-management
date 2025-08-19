import streamlit as st 
import os 
from dotenv import load_dotenv 
from datetime import datetime

import sys
from pathlib import Path

# Add the project root to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))  # Goes up from app/ to water management/

# Add dashwork and analytics to Python's module search path
sys.path.append(str(Path(__file__).parent.parent / "dashwork.dash"))
sys.path.append(str(Path(__file__).parent.parent / "analytics.stranalytics"))
sys.path.append(str(Path(__file__).parent.parent / "analytics.chatbot"))


from dashwork.dash import show_dashboard
from analytics.stranalytics import show_analytics
from analytics.chatbot import show_chatbot
from app.config import CONFIG
from dashwork.dash import show_alerts


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

# --- CSS styling with updated header --- 
st.markdown(""" 
<style> 
    .main-header { 
        background: linear-gradient(90deg, #4facfe 0%, #00f2fe 100%); 
        color: white; 
        padding: 20px; 
        border-radius: 10px; 
        margin-bottom: 20px; 
        text-align: center; 
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
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

def main(): 
    if "data_mode" not in st.session_state: 
        st.session_state.data_mode = 'historical' 
     
    st.markdown(""" 
    <div class="main-header"> 
        <h1 style="color: white;">🌊 Water Management System</h1> 
        <p style="color: white;">Real-time monitoring, analytics, and AI-powered insights for water quality management</p> 
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
         
        if st.button("🔄 Refresh Data", use_container_width=True, key="main_refresh"): 
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