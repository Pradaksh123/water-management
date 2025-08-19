import streamlit as st 
import plotly.express as px 
import pandas as pd 
from supabase import create_client 
import os 
from datetime import datetime 
from dotenv import load_dotenv
from app.config import CONFIG

# Initialize environment variables
load_dotenv()

def initialize_supabase():
    """Initialize and return Supabase client with error handling"""
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_ANON_KEY")
        
        if not supabase_url or not supabase_key:
            st.error("Supabase credentials not configured")
            return None
            
        return create_client(supabase_url, supabase_key)
    except Exception as e:
        st.error(f"Failed to initialize Supabase: {str(e)}")
        return None

def fetch_data_with_pagination(table_name, supabase_client, start_date=None, end_date=None):
    """Fetch data with pagination handling"""
    all_data = []
    page_size = 1000
    offset = 0
    
    try:
        while True:
            query = supabase_client.table(table_name).select('*')
            
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
        st.error(f"Pagination fetch error for {table_name}: {str(e)}")
        return pd.DataFrame()

def fetch_analytics_data():
    """Fetch analytics data with comprehensive error handling and pagination"""
    supabase = initialize_supabase()
    if not supabase:
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        # Get quality data for June 2025 with pagination
        quality_df = fetch_data_with_pagination(
            'water_quality',
            supabase,
            datetime(2025, 6, 1),
            datetime(2025, 7, 1)
        )
        
        # Get flow data for June 2025 with pagination
        flow_df = fetch_data_with_pagination(
            'flow_rate',
            supabase,
            datetime(2025, 6, 1),
            datetime(2025, 7, 1)
        )
            
        return quality_df, flow_df
        
    except Exception as e:
        st.error(f"Data fetch error: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

def clean_quality_data(quality_df):
    """Clean and validate quality data"""
    if quality_df.empty:
        return quality_df
    
    # Validate required columns
    required_columns = ['value', 'parameter_name']
    missing_columns = [col for col in required_columns if col not in quality_df.columns]
    
    if missing_columns:
        st.error(f"Missing required columns: {', '.join(missing_columns)}")
        return pd.DataFrame()
    
    # Clean the data
    quality_df_clean = quality_df.copy()
    quality_df_clean['value'] = pd.to_numeric(quality_df_clean['value'], errors='coerce')
    quality_df_clean = quality_df_clean.dropna(subset=['value', 'parameter_name'])
    
    return quality_df_clean

def render_parameter_trends(filtered_quality_df):
    """Render parameter trends visualization"""
    if filtered_quality_df.empty:
        st.info("No data available for trend analysis")
        return
        
    filtered_quality_df['datetime'] = pd.to_datetime(filtered_quality_df['timestamp'], format='mixed')
    fig = px.line(
        filtered_quality_df, 
        x='datetime', 
        y='value', 
        color='parameter_name',
        title='Selected Parameter Trends Over Time',
        labels={'value': 'Parameter Value', 'datetime': 'Date'}
    )
    st.plotly_chart(fig, use_container_width=True)

def render_statistical_summary(filtered_quality_df):
    """Render statistical analysis section"""
    if filtered_quality_df.empty:
        st.info("No data available for statistical analysis")
        return
    
    # Basic statistics
    stats_summary = filtered_quality_df.groupby('parameter_name')['value'].agg([
        'count', 'mean', 'std', 'min', 'max', 'median'
    ]).round(2)
    stats_summary.columns = ['Count', 'Mean', 'Std Dev', 'Min', 'Max', 'Median']
    st.dataframe(stats_summary, use_container_width=True)
    
    # Compliance analysis
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
        st.markdown("#### Compliance Analysis")
        compliance_df = pd.DataFrame(compliance_data)
        st.dataframe(compliance_df, use_container_width=True)

def render_correlation_analysis(filtered_quality_df):
    """Render parameter correlation analysis"""
    if len(filtered_quality_df['parameter_name'].unique()) <= 1:
        st.info("Need at least 2 parameters for correlation analysis")
        return
        
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
        st.info("Not enough data for correlation analysis")

def render_flow_analysis(filtered_flow_df, selected_locations):
    """Render flow rate analysis by location"""
    if not filtered_flow_df.empty and selected_locations:
        loc_summary = filtered_flow_df.groupby('location_name')['totalizer'].mean().reset_index()
        fig = px.bar(
            loc_summary,
            x='location_name',
            y='totalizer',
            color='location_name',
            title='Average Flow Values by Location',
            labels={'totalizer': 'Average Flow Rate', 'location_name': 'Location'}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Please select at least one location for comparison")

def show_analytics():
    """Main function to display analytics dashboard"""
    st.markdown("### 📊 Advanced Analytics")
    
    # Fetch and clean data (now with pagination)
    quality_df, flow_df = fetch_analytics_data()
    quality_df_clean = clean_quality_data(quality_df)
    
    if quality_df_clean.empty or flow_df.empty:
        st.warning("Insufficient data for analysis")
        return
    
    # Display data info with record counts
    with st.expander("Data Summary", expanded=False):
        st.write(f"Total quality records: {len(quality_df_clean)}")
        st.write(f"Total flow records: {len(flow_df)}")
        st.write("Sample quality data:", quality_df_clean.head(3))
        st.write("Sample flow data:", flow_df.head(3) if not flow_df.empty else "No flow data")
    
    # Create control widgets
    col1, col2, col3 = st.columns(3)
    
    with col1:
        parameters = quality_df_clean['parameter_name'].unique()
        parameters_list = list(parameters) if hasattr(parameters, '__array__') else parameters
        selected_params = st.multiselect(
            "Select Parameters",
            parameters_list,
            default=parameters_list[:3] if len(parameters_list) > 3 else parameters_list
        )
    
    with col2:
        locations = flow_df['location_name'].unique()
        locations_list = list(locations) if hasattr(locations, '__array__') else locations
        selected_locations = st.multiselect(
            "Select Locations",
            locations_list,
            default=locations_list[:2] if len(locations_list) > 0 else []
        )
    
    with col3:
        time_range = st.selectbox(
            "Time Range",
            ["Last 7 days", "Last 30 days", "All data"],
            index=2  # Default to "All data"
        )
    
    # Filter data based on selections
    filtered_quality_df = quality_df_clean[
        quality_df_clean['parameter_name'].isin(selected_params)
    ] if selected_params else quality_df_clean
    
    filtered_flow_df = flow_df[
        flow_df['location_name'].isin(selected_locations)
    ] if selected_locations else flow_df
    
    st.divider()
    
    # Create analysis tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Trends", 
        "📊 Statistics", 
        "🔗 Correlations", 
        "📍 Location Comparison"
    ])
    
    with tab1:
        render_parameter_trends(filtered_quality_df)
    
    with tab2:
        render_statistical_summary(filtered_quality_df)
    
    with tab3:
        render_correlation_analysis(filtered_quality_df)
    
    with tab4:
        st.markdown("#### Parameter-Location Comparison")
        st.warning("Water quality parameters cannot be compared by location as location data is not available in the water_quality table.")
        render_flow_analysis(filtered_flow_df, selected_locations)

# For testing the component standalone
if __name__ == "__main__":
    show_analytics()