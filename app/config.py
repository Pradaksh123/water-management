import os
from dotenv import load_dotenv

load_dotenv()

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