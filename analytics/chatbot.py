import streamlit as st 
from supabase import create_client 
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate 
from langchain_core.output_parsers import StrOutputParser 
import os 
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import re
import hashlib
from app.config import CONFIG

load_dotenv()

class WaterChatBot: 
    def __init__(self): 
        self.supabase = None
        self.llm = None
        self.chain = None
        self.initialized = False
        self.init_error = None
        self.query_cache = {}  # Cache for frequent queries
        
    def initialize(self):
        """Initialize the chatbot components with proper error handling"""
        try:
            # Initialize Supabase client
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_ANON_KEY")
            if not supabase_url or not supabase_key:
                raise ValueError("Supabase credentials not found")
            
            self.supabase = create_client(supabase_url, supabase_key)
            
            # Initialize Gemini model
            gemini_key = os.getenv("GEMINI_API_KEY")
            if not gemini_key:
                raise ValueError("Gemini API key not found")
                
            self.llm = ChatGoogleGenerativeAI(
                model="gemini-1.5-flash",
                google_api_key=gemini_key,
                temperature=0.3
            )
            
            # Set up the prompt template
            self.prompt_template = ChatPromptTemplate.from_messages([
                ("system", """
                You are an expert water management system analyst with access to:
                1. Water quality data (water_quality table)
                2. Flow rate data (flow_rate table)
                
                Database Schema:
                - water_quality: timestamp, parameter_name, value
                - flow_rate: timestamp, location_name, totalizer
                
                Strict Guidelines:
                1. Always check the database first for any query
                2. For real-time alerts, refer to the alerts monitoring system
                3. For historical data (June-July 2025), query the Supabase database
                4. When presenting data, strictly follow these table formats:
                   - For water quality data (only these columns):
                     | Timestamp            | Parameter | Value | Status       |
                     |----------------------|-----------|-------|--------------|
                     | 2025-06-15 08:30:00 | pH        | 7.2   | Normal       |
                     | 2025-06-15 09:45:00 | TDS       | 850   | Normal       |
                     | 2025-06-15 10:15:00 | BOD       | 6.2   | Out of Range |
                   - For flow data (only these columns):
                     | Timestamp            | Location              | Totalizer |
                     |----------------------|-----------------------|-----------|
                     | 2025-06-15 08:30:00 | Ground Water Source 1 | 4500      |
                     | 2025-06-15 09:45:00 | Industrial Process    | 3200      |
                5. Never add columns that don't exist in the original tables
                6. For quality data, mark status as:
                   - "Normal" when within safe ranges
                   - "Out of Range" when outside safe ranges
                7. For flow data, only show the data without status indicators
                8. Always include 6-7 representative entries in tables
                9. Safe ranges for quality parameters:
                   {safe_ranges}
                """),
                ("human", """
                Question: {question}
                
                Current Alerts: {alerts}
                
                Data Context (CSV):
                {context}
                """)
            ])
            
            # Create the processing chain
            self.chain = self.prompt_template | self.llm | StrOutputParser()
            self.initialized = True
            
        except Exception as e:
            self.init_error = str(e)
            self.initialized = False

    def parse_query_intent(self, question):
        """
        Parse the user's question to determine what data to fetch
        Returns: dict with table, filters, and time_range
        """
        question_lower = question.lower()
        result = {
            "table": "water_quality",  # default table
            "filters": {},
            "time_range": "full",  # full, recent, or specific
            "aggregation": None,  # latest, average, etc.
            "needs_full_scan": False  # Flag for whether we need complete data
        }
        
        # Determine which table to query
        if any(word in question_lower for word in ["flow", "totalizer", "location"]):
            result["table"] = "flow_rate"
        
        # Check for specific time references
        time_keywords = {
            "today": timedelta(hours=24),
            "yesterday": timedelta(hours=48),
            "last hour": timedelta(hours=1),
            "last 24 hours": timedelta(hours=24),
            "last week": timedelta(weeks=1),
            "recent": timedelta(hours=6),
            "now": timedelta(hours=1),
            "current": timedelta(hours=1)
        }
        
        for keyword, delta in time_keywords.items():
            if keyword in question_lower:
                result["time_range"] = ("relative", datetime.now() - delta)
                break
                
        # Check for specific date mentions
        date_pattern = r'(\d{4}-\d{2}-\d{2})|(june \d{1,2}|july \d{1,2})'
        date_match = re.search(date_pattern, question_lower)
        if date_match:
            result["time_range"] = ("absolute", date_match.group(0))
        
        # Check for aggregation requests
        if any(word in question_lower for word in ["latest", "current", "now", "recent"]):
            result["aggregation"] = "latest"
        elif "average" in question_lower or "mean" in question_lower:
            result["aggregation"] = "average"
        elif "trend" in question_lower:
            result["aggregation"] = "trend"
            
        # Extract specific parameters for water quality
        if result["table"] == "water_quality":
            params = []
            for param in CONFIG["PARAMETERS"]:
                if param.lower() in question_lower:
                    params.append(param)
            if params:
                result["filters"]["parameter_name"] = params
                
        # Extract locations for flow data
        if result["table"] == "flow_rate":
            # You would need to define known locations in your CONFIG
            known_locations = ["Ground Water Source 1", "Industrial Process", "Treatment Plant Inlet"]
            locations = []
            for loc in known_locations:
                if loc.lower() in question_lower:
                    locations.append(loc)
            if locations:
                result["filters"]["location_name"] = locations
                
        # Determine if we need a full database scan
        # Questions about trends, patterns, or comprehensive analysis need full data
        needs_full_scan_keywords = [
            "trend", "pattern", "over time", "historical", "all data", 
            "complete", "entire", "whole", "every", "each", "summary",
            "overview", "analysis", "report"
        ]
        
        if any(keyword in question_lower for keyword in needs_full_scan_keywords):
            result["needs_full_scan"] = True
            result["time_range"] = "full"
                
        return result

    def fetch_data(self, table, intent):
        """
        Fetch data from Supabase based on query intent
        Uses intelligent filtering to minimize data transfer when possible
        but will scan the entire database when needed
        """
        if not self.initialized or not self.supabase:
            return pd.DataFrame()
            
        # Create a cache key to avoid duplicate queries
        cache_key = hashlib.md5(f"{table}_{str(intent)}".encode()).hexdigest()
        
        # Return cached result if available (valid for 5 minutes)
        if cache_key in self.query_cache:
            cached_time, cached_data = self.query_cache[cache_key]
            if (datetime.now() - cached_time).total_seconds() < 300:  # 5 minute cache
                return cached_data
        
        try:
            # Build base query
            query = self.supabase.table(table).select('*')
            
            # Apply time filters based on intent
            if intent["time_range"] != "full":
                if intent["time_range"][0] == "relative":
                    start_time = intent["time_range"][1]
                    query = query.gte('timestamp', start_time.isoformat())
                elif intent["time_range"][0] == "absolute":
                    # Parse absolute date - this is simplified
                    query = query.gte('timestamp', '2025-06-01').lte('timestamp', '2025-06-02')
            else:
                # Full date range for comprehensive analysis
                query = query.gte('timestamp', '2025-06-01').lte('timestamp', '2025-07-01')
            
            # Apply additional filters
            for field, values in intent["filters"].items():
                query = query.in_(field, values)
                
            # For latest queries, get only the most recent records
            if intent["aggregation"] == "latest":
                query = query.order('timestamp', desc=True).limit(10)
                result = query.execute()
                df = pd.DataFrame(result.data) if result.data else pd.DataFrame()
            else:
                # For comprehensive queries, use proper pagination to get ALL data
                all_data = []
                page_size = 1000  # Use larger pages for full scans to reduce API calls
                offset = 0
                
                while True:
                    result = query.range(offset, offset + page_size - 1).execute()
                    page_data = result.data
                    
                    if not page_data:
                        break
                        
                    all_data.extend(page_data)
                    offset += page_size
                    
                    # If we don't need a full scan, break early with enough data
                    if not intent["needs_full_scan"] and len(all_data) >= 500:
                        break
                    
                    if len(page_data) < page_size:
                        break
                
                df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
            
            # Cache the result
            self.query_cache[cache_key] = (datetime.now(), df)
            return df
            
        except Exception as e:
            st.error(f"Data fetch error: {str(e)}")
            return pd.DataFrame()

    def check_alerts(self):
        """Check for parameter alerts with validation - optimized version"""
        alerts = []
        try:
            # Only check recent data for alerts (last 24 hours)
            intent = {
                "table": "water_quality",
                "filters": {},
                "time_range": ("relative", datetime.now() - timedelta(hours=24)),
                "aggregation": None,
                "needs_full_scan": False
            }
            
            quality_df = self.fetch_data('water_quality', intent)
            if not quality_df.empty:
                quality_df['value'] = pd.to_numeric(quality_df['value'], errors='coerce')
                quality_df.dropna(subset=['value'], inplace=True)

                for _, row in quality_df.iterrows():
                    param = row['parameter_name']
                    if param in CONFIG["SAFE_RANGES"]:
                        min_val, max_val = CONFIG["SAFE_RANGES"][param]
                        value = row['value']
                        if not (min_val <= value <= max_val):
                            alerts.append(f"{param}: {value} (Safe: {min_val}-{max_val})")
                            
            # Limit alerts to prevent prompt overload
            alerts = alerts[:5]
            
        except Exception as e:
            st.error(f"Alert check failed: {str(e)}")
            
        return alerts

    def generate_response(self, question):
        """Generate response with comprehensive error handling and optimized queries"""
        if not self.initialized:
            return "❌ Chatbot is not properly initialized."
            
        try:
            # Parse the question to understand what data is needed
            intent = self.parse_query_intent(question)
            
            alerts = self.check_alerts()
            alert_status = "✅ All parameters normal" if not alerts else "\n".join(alerts)
            
            # Format safe ranges for the prompt
            safe_ranges = "\n".join([f"{k}: {v}" for k, v in CONFIG["SAFE_RANGES"].items()])
            
            # Fetch only the needed data
            df = self.fetch_data(intent["table"], intent)
            
            if df.empty:
                return "No relevant data found for your query."
                
            # If user asked for an aggregation, pre-calculate it
            context_data = df.to_csv()
            if intent["aggregation"] == "average" and not df.empty and 'value' in df.columns:
                avg_value = df['value'].mean()
                context_data = f"The average value is: {avg_value:.2f}\n\nSample Data:\n" + df.head(10).to_csv()
            elif intent["aggregation"] == "latest" and not df.empty:
                context_data = "Latest readings:\n" + df.to_csv()
            elif intent["needs_full_scan"] and len(df) > 1000:
                # For large datasets, provide a summary instead of all data
                summary = f"Comprehensive analysis of {len(df)} records. Key insights:\n"
                if 'value' in df.columns:
                    summary += f"- Values range from {df['value'].min():.2f} to {df['value'].max():.2f}\n"
                    summary += f"- Average value: {df['value'].mean():.2f}\n"
                summary += f"- Time range: {df['timestamp'].min()} to {df['timestamp'].max()}\n"
                context_data = summary + "\nSample data:\n" + df.sample(min(10, len(df))).to_csv()
                
            return self.chain.invoke({
                "question": question,
                "context": context_data,
                "alerts": alert_status,
                "safe_ranges": safe_ranges
            })
            
        except Exception as e:
            return f"❌ Error generating response: {str(e)}"

def show_chatbot(initial_question=None):
    """Main function to display the chatbot interface"""
    # Check for required environment variables
    required_vars = {
        "SUPABASE_URL": "Supabase URL",
        "SUPABASE_ANON_KEY": "Supabase Key",
        "GEMINI_API_KEY": "Gemini API Key"
    }
    
    missing = [name for var, name in required_vars.items() if not os.getenv(var)]
    if missing:
        st.error(f"Missing required configuration: {', '.join(missing)}")
        st.info("Please check your environment variables or .env file")
        return
    
    # Initialize chatbot in session state
    if "chatbot" not in st.session_state:
        st.session_state.chatbot = WaterChatBot()
        with st.spinner("Initializing AI chatbot..."):
            st.session_state.chatbot.initialize()
        
        if not st.session_state.chatbot.initialized:
            st.error(f"Chatbot initialization failed: {st.session_state.chatbot.init_error}")
            return
    
    # Initialize chat history if not exists
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    
    # Handle initial question if provided
    if initial_question and initial_question not in [msg["content"] for msg in st.session_state.chat_history]:
        st.session_state.chat_history.append({"role": "user", "content": initial_question})
        with st.chat_message("user"):
            st.markdown(initial_question)
        
        response = st.session_state.chatbot.generate_response(initial_question)
        st.session_state.chat_history.append({"role": "assistant", "content": response})
    
    # Display chat history
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
    
    # Handle new user input
    if prompt := st.chat_input("Ask about water data..."):
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                response = st.session_state.chatbot.generate_response(prompt)
                st.markdown(response)
        
        st.session_state.chat_history.append({"role": "assistant", "content": response})

# For testing the component standalone
if __name__ == "__main__":
    show_chatbot()