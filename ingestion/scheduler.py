import time
from datetime import datetime
from supabase import create_client
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import re
from collections import defaultdict
import random

load_dotenv()

class RemainingDataIngestor:
    def __init__(self):
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_ANON_KEY")
        )
        self.parameters = [
            "HUMIDITY", "ETP (TDS)", "ETP (pH)",
            "STP (TDS)", "STP (TSS)", "STP (BOD)",
            "STP (pH)", "STP (COD)"
        ]
        self.locations = [
            "Corporation Water",
            "Ground Water Source 1",
            "Ground Water Source 2",
            "Industrial Process",
            "Tanker Water Supply"
        ]
        self.safe_ranges = {
            "HUMIDITY": (30, 70),
            "ETP (TDS)": (100, 1000),
            "ETP (pH)": (6.5, 9.0),
            "STP (TDS)": (100, 1000),
            "STP (TSS)": (1000, 3000),
            "STP (BOD)": (0, 5),
            "STP (pH)": (6.5, 9.0),
            "STP (COD)": (1000, 3000)
        }

    def normalize_parameter_name(self, raw_name):
        """Identical to your main ingestion logic"""
        cleaned = re.sub(r'^\d+\.\s*', '', raw_name.strip().rstrip(','))
        if 'Safe Range:' in cleaned:
            cleaned = cleaned.split('Safe Range:')[0].strip()
        cleaned = re.sub(r'[^\w\s()]', '', cleaned).strip()
        
        name_mapping = {
            'ETP TDS': 'ETP (TDS)', 'ETP pH': 'ETP (pH)',
            'STP TDS': 'STP (TDS)', 'STP TSS': 'STP (TSS)',
            'STP BOD': 'STP (BOD)', 'STP pH': 'STP (pH)',
            'STP COD': 'STP (COD)', 'HUMIDITY': 'HUMIDITY',
            'ETP(TDS)': 'ETP (TDS)', 'ETP(pH)': 'ETP (pH)',
            'STP(TDS)': 'STP (TDS)', 'STP(TSS)': 'STP (TSS)',
            'STP(BOD)': 'STP (BOD)', 'STP(pH)': 'STP (pH)',
            'STP(COD)': 'STP (COD)'
        }
        return name_mapping.get(cleaned, cleaned)

    def get_remaining_data_chunks(self, file_path, chunk_size=10):
        """Yield chunks of remaining 20% data per parameter/location"""
        df = pd.read_csv(file_path, header=None)
        total_rows = len(df)
        start_idx = int(total_rows * 0.8)  # Start after 80% mark
        
        # Process identical to main ingestion but for remaining data
        rows_by_param_loc = defaultdict(list)
        current_param = None
        safe_min = safe_max = None

        for idx in range(start_idx, total_rows):
            row = df.iloc[idx]
            line = str(row[0])

            if pd.isna(row[0]):
                continue

            if "Safe Range:" in line:
                param_line = line.split("Safe Range:")[0].strip()
                current_param = self.normalize_parameter_name(param_line)
                if not current_param:
                    continue
                safe_range_str = line.split("Safe Range:")[1].strip(" ()")
                safe_min, safe_max = map(float, safe_range_str.split("to"))
                continue

            if any(x in line.lower() for x in ["date", "time", "parameter"]):
                continue

            try:
                if not current_param or pd.isna(row[0]) or pd.isna(row[1]) or pd.isna(row[2]):
                    continue
                
                timestamp = pd.to_datetime(f"{row[0]} {row[1]}").isoformat()
                value = float(row[2])
                if not np.isfinite(value):
                    continue

                # Distribute across locations
                for location in self.locations:
                    rows_by_param_loc[(current_param, location)].append({
                        "timestamp": timestamp,
                        "parameter_name": current_param,
                        "location_name": location, # Added location to the data for upsert
                        "value": value * (0.9 + 0.2 * random.random()),  # Add slight variation
                        "safe_min": safe_min,
                        "safe_max": safe_max
                    })

            except Exception:
                continue

        # Yield chunks per parameter-location combination
        for (param, loc), records in rows_by_param_loc.items():
            for i in range(0, len(records), chunk_size):
                yield records[i:i + chunk_size]

    def process_flow_chunks(self, file_path, chunk_size=10):
        """Yield chunks of remaining 20% flow data"""
        df = pd.read_csv(file_path, header=None)
        total_rows = len(df)
        start_idx = int(total_rows * 0.8)
        
        rows_by_loc = defaultdict(list)
        current_location = None

        for idx in range(start_idx, total_rows):
            row = df.iloc[idx]
            col_a, col_b, col_c = str(row[0]), str(row[1]), str(row[2])

            if col_a.startswith("Location Name:"):
                current_location = col_a.replace("Location Name:", "").strip()
                continue

            if not all([col_a, col_b, col_c]) or col_a == "########":
                continue

            try:
                timestamp = pd.to_datetime(f"{col_a} {col_b}").isoformat()
                totalizer = float(col_c)
                rows_by_loc[current_location].append({
                    "timestamp": timestamp,
                    "location_name": current_location,
                    "totalizer": totalizer
                })
            except Exception:
                continue

        for loc, records in rows_by_loc.items():
            for i in range(0, len(records), chunk_size):
                yield records[i:i + chunk_size]

    def ingest_batch(self, batch, table_name):
        """Insert or update batch with error handling"""
        try:
            # Upsert will update existing rows or insert new ones
            response = self.supabase.table(table_name).upsert(batch).execute()
            print(f"✅ Upserted {len(batch)} records to {table_name}")
            # Check alerts is still useful for logging
            self.check_alerts(batch) if table_name == "water_quality" else None
            return True
        except Exception as e:
            print(f"❌ Failed to upsert batch: {e}")
            return False

    def check_alerts(self, batch):
        """Check for parameter violations"""
        for record in batch:
            param = record['parameter_name']
            value = record['value']
            # Note: Your CSV data is missing location_name for quality data.
            # I added it to the `get_remaining_data_chunks` function to make this log useful.
            location = record.get('location_name', 'N/A') 
            min_val, max_val = self.safe_ranges.get(param, (None, None))
            if min_val and (value < min_val or value > max_val):
                print(f"🚨 ALERT: {param} = {value} (Safe: {min_val}-{max_val}) at {location}")

    def run_ingestion_cycle(self):
        """Process remaining 20% in chunks every 45 seconds"""
        print("\n=== Starting 20% Data Ingestion ===")
        
        # Process quality data
        quality_chunks = self.get_remaining_data_chunks("data/water_quality_data.csv")
        for chunk in quality_chunks:
            if chunk:
                self.ingest_batch(chunk, "water_quality")
        
        # Process flow data
        flow_chunks = self.process_flow_chunks("data/water_flow_data.csv")
        for chunk in flow_chunks:
            if chunk:
                self.ingest_batch(chunk, "flow_rate")
        
        print("=== Cycle Complete ===\n")

def start_scheduler():
    """Start the scheduler for demo purposes"""
    ingestor = RemainingDataIngestor()
    while True:
        ingestor.run_ingestion_cycle()
        time.sleep(45)  # 45-second interval for demo

if __name__ == "__main__":
    start_scheduler()