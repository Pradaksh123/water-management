import streamlit as st
from supabase import create_client, Client
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import os
from datetime import datetime
import pandas as pd

# Configuration - Update these to match your Supabase schema
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
    },
    "MODEL_NAME": "gemini-1.5-flash",  # Updated to Gemini 1.5 Pro
    "MODEL_KWARGS": {
        "temperature": 0.3,
        "top_p": 0.95,
        "max_output_tokens": 8192  # Leverage 1.5 Pro's larger capacity
    }
}

class WaterChatBot:
    def __init__(self):
        self.supabase = create_client(CONFIG["SUPABASE_URL"], CONFIG["SUPABASE_KEY"])
        self.llm = ChatGoogleGenerativeAI(
            model=CONFIG["MODEL_NAME"],
            google_api_key=CONFIG["GEMINI_API_KEY"],
            model_kwargs=CONFIG["MODEL_KWARGS"]
        )
        self.prompt_template = ChatPromptTemplate.from_messages([
            ("system", """
            You are a water management expert analyzing data from June 1 to July 1 2025.
            Current alerts: {alerts}
            
            Rules:
            1. Use only the provided CSV data
            2. Always mention location when available
            3. Highlight values outside safe ranges
            4. Cross-reference multiple parameters when relevant
            5. Provide actionable recommendations
            """),
            ("human", """
            Question: {question}
            
            Data Context (CSV):
            {context}
            """)
        ])
        self.chain = self.prompt_template | self.llm | StrOutputParser()

    def fetch_data(self, table, params=None, locations=None, limit=5000):
        """Fetch data from specified table with filters"""
        query = self.supabase.table(table) \
            .select('*') \
            .gte('timestamp', datetime(2025, 6, 1).isoformat()) \
            .lte('timestamp', datetime(2025, 7, 1).isoformat()) \
            .limit(limit)  # Increased limit for 1.5 Pro's larger context

        if params:
            query = query.in_('parameter_name', params)
        if locations:
            query = query.in_('location_name', locations)

        result = query.execute()
        return pd.DataFrame(result.data) if result.data else pd.DataFrame()

    def check_alerts(self):
        """Check for parameter violations with severity levels"""
        alerts = []
        df = self.fetch_data(CONFIG["TABLES"]["quality"])
        
        if not df.empty:
            for _, row in df.iterrows():
                param = row['parameter_name']
                min_val, max_val = CONFIG["SAFE_RANGES"].get(param, (None, None))
                if min_val and (row['value'] < min_val or row['value'] > max_val):
                    severity = "CRITICAL" if (row['value'] < min_val*0.8 or row['value'] > max_val*1.2) else "WARNING"
                    alerts.append({
                        "location": row.get('location_name', 'Unknown'),
                        "parameter": param,
                        "value": row['value'],
                        "range": f"{min_val}-{max_val}",
                        "severity": severity
                    })
        return alerts

    def generate_response(self, question):
        """Generate response with enhanced data analysis"""
        alerts = self.check_alerts()
        alert_status = (
            "✅ All parameters normal" if not alerts 
            else "\n".join(
                f"⚠️ {a['severity']}: {a['location']} - {a['parameter']} = {a['value']} (Safe: {a['range']})"
                for a in alerts
            )
        )
        
        # Smart data fetching based on question
        if "flow" in question.lower():
            df = self.fetch_data(CONFIG["TABLES"]["flow"])
            context_cols = ["timestamp", "location_name", "totalizer"]
        else:
            params = [p for p in CONFIG["PARAMETERS"] if p.lower() in question.lower() or "all" in question.lower()]
            locs = [l for l in CONFIG["LOCATIONS"] if l.lower() in question.lower() or "all" in question.lower()]
            df = self.fetch_data(
                CONFIG["TABLES"]["quality"],
                params=params if params else None,
                locations=locs if locs else None
            )
            context_cols = ["timestamp", "location_name", "parameter_name", "value", "safe_min", "safe_max"]
        
        if df.empty:
            return "No relevant data found for your query."
        
        # Prepare context efficiently
        context_df = df[context_cols].drop_duplicates()
        
        try:
            return self.chain.invoke({
                "question": question,
                "context": context_df.to_csv(index=False),
                "alerts": alert_status
            })
        except Exception as e:
            st.error(f"Model error: {str(e)}")
            return "Sorry, I encountered an error processing your request."

def show_chatbot(initial_question=None):
    """Enhanced chatbot interface with memory"""
    chatbot = WaterChatBot()
    
    # Initialize session state
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
        st.session_state.last_response = None
    
    # Handle initial question
    if initial_question and initial_question not in [msg["content"] for msg in st.session_state.chat_history]:
        st.session_state.chat_history.append({"role": "user", "content": initial_question})
        response = chatbot.generate_response(initial_question)
        st.session_state.chat_history.append({"role": "assistant", "content": response})
        st.session_state.last_response = response
    
    # Display chat history with improved formatting
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                st.markdown(msg["content"])
                if "CRITICAL" in msg["content"]:
                    st.error("Immediate attention required!")
                elif "WARNING" in msg["content"]:
                    st.warning("Parameter out of range")
            else:
                st.markdown(msg["content"])
    
    # Chat input with follow-up suggestions
    if prompt := st.chat_input("Ask about water quality or flow data..."):
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("assistant"):
            with st.spinner("Analyzing water data..."):
                response = chatbot.generate_response(prompt)
                st.markdown(response)
                st.session_state.last_response = response
        st.session_state.chat_history.append({"role": "assistant", "content": response})

    # Add follow-up question suggestions
    if st.session_state.last_response:
        st.sidebar.markdown("**Suggested follow-ups**")
        suggestions = [
            "Show me trends for the worst parameter",
            "Compare Industrial Process with Corporation Water",
            "What maintenance would you recommend?"
        ]
        for q in suggestions:
            if st.sidebar.button(q):
                st.session_state.chat_history.append({"role": "user", "content": q})
                with st.chat_message("assistant"):
                    with st.spinner("Analyzing..."):
                        response = chatbot.generate_response(q)
                        st.markdown(response)
                st.session_state.chat_history.append({"role": "assistant", "content": response})
                st.rerun()