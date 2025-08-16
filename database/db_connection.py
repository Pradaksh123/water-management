import os
from dotenv import load_dotenv
import psycopg2
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

def get_connection():
    try:
        db_url = os.getenv("SUPABASE_DB_URL")
        if not db_url:
            raise ValueError("Database URL not found. Please set SUPABASE_DB_URL in .env")
        
        conn = psycopg2.connect(db_url)
        logging.info("✅ Connected to PostgreSQL database successfully.")
        return conn
    except Exception as e:
        logging.error(f"❌ Error connecting to database: {e}")
        return None

# Example usage
if __name__ == "__main__":
    conn = get_connection()
    if conn:
        conn.close()
